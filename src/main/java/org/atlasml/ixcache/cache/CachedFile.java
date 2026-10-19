package org.atlasml.ixcache.cache;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.LongConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Block store for one cached file: a sparse data file plus a block bitmap.
 *
 * <p>A block is either present (its bytes are on disk), in flight (an origin
 * fetch or the following disk write is outstanding) or missing. Readers that
 * need an in-flight block wait on the same future instead of fetching it again,
 * and receive bytes as soon as the origin delivers them. A block is marked
 * present only after its disk write returns, and metadata is written only after
 * the data file is forced, so persisted metadata never claims unwritten blocks.
 */
final class CachedFile {

    private static final Logger log = LoggerFactory.getLogger(CachedFile.class);

    private final Path metaPath;
    private final String key;
    private final String originUrl;
    private final long fileSize;
    private final int blockSize;
    private final int maxRunBlocks;
    private final FileChannel data;
    private final Executor io;
    private final LongConsumer onPublished;

    private final Object lock = new Object();
    private final Object flushLock = new Object();
    private final BitSet present; // guarded by lock
    private final Map<Integer, CompletableFuture<ByteBuffer>> inflight = new HashMap<>(); // guarded by lock
    private long lastAccess; // guarded by lock
    private boolean dirty; // guarded by lock
    private boolean closed; // guarded by lock

    private CachedFile(Path dataPath, Path metaPath, FileMeta meta, int maxRequestBytes,
            Executor io, LongConsumer onPublished) throws IOException {
        this.metaPath = metaPath;
        this.key = meta.key();
        this.originUrl = meta.originUrl();
        this.fileSize = meta.fileSize();
        this.blockSize = meta.blockSize();
        this.maxRunBlocks = Math.max(1, maxRequestBytes / blockSize);
        this.present = (BitSet) meta.present().clone();
        this.lastAccess = meta.lastAccess();
        this.io = io;
        this.onPublished = onPublished;
        this.data = FileChannel.open(dataPath, StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE);
    }

    /**
     * Opens the block store described by {@code meta}. {@code onPublished} is
     * called with the byte count of every run of blocks that becomes present.
     */
    static CachedFile open(Path dataPath, Path metaPath, FileMeta meta, int maxRequestBytes,
            Executor io, LongConsumer onPublished, boolean isNew) throws IOException {
        CachedFile f = new CachedFile(dataPath, metaPath, meta, maxRequestBytes, io, onPublished);
        if (isNew) {
            synchronized (f.lock) {
                f.dirty = true;
            }
            f.flush();
        }
        return f;
    }

    long fileSize() {
        return fileSize;
    }

    int blockSize() {
        return blockSize;
    }

    void touch(long now) {
        synchronized (lock) {
            lastAccess = now;
            dirty = true;
        }
    }

    /**
     * Reads {@code requested} ranges, serving present blocks from disk and
     * fetching missing ones from {@code origin}. Ranges past end of file are
     * truncated.
     */
    CompletableFuture<ReadResult> read(List<Range> requested, OriginReader origin) {
        List<Range> ranges = requested.stream().map(r -> r.clamp(fileSize)).toList();
        ByteBuffer[] out = new ByteBuffer[ranges.size()];
        TreeSet<Integer> needed = new TreeSet<>();
        for (int i = 0; i < ranges.size(); i++) {
            Range r = ranges.get(i);
            out[i] = ByteBuffer.allocate(r.length());
            if (r.length() > 0) {
                for (int b = blockOf(r.offset()); b <= blockOf(r.end() - 1); b++) {
                    needed.add(b);
                }
            }
        }

        BitSet hit = new BitSet();
        Map<Integer, CompletableFuture<ByteBuffer>> pending = new HashMap<>();
        List<Integer> toFetch = new ArrayList<>();
        synchronized (lock) {
            if (closed) {
                return CompletableFuture.failedFuture(new IOException("cache file closed: " + key));
            }
            for (int b : needed) {
                if (present.get(b)) {
                    hit.set(b);
                    continue;
                }
                CompletableFuture<ByteBuffer> f = inflight.get(b);
                if (f == null) {
                    f = new CompletableFuture<>();
                    inflight.put(b, f);
                    toFetch.add(b);
                }
                pending.put(b, f);
            }
        }

        long fetched = fetch(toFetch, pending, origin);

        long bytesHit;
        try {
            bytesHit = readHits(ranges, out, hit);
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }

        if (pending.isEmpty()) {
            return CompletableFuture.completedFuture(new ReadResult(List.of(out), bytesHit, fetched));
        }
        return CompletableFuture.allOf(pending.values().toArray(CompletableFuture[]::new))
                .thenApply(v -> {
                    copyPending(ranges, out, pending);
                    return new ReadResult(List.of(out), bytesHit, fetched);
                });
    }

    /** Requests the blocks in {@code toFetch} (ascending) from the origin. Returns bytes requested. */
    private long fetch(List<Integer> toFetch, Map<Integer, CompletableFuture<ByteBuffer>> futures,
            OriginReader origin) {
        if (toFetch.isEmpty()) {
            return 0;
        }
        List<int[]> runs = new ArrayList<>(); // {firstBlock, blockCount}
        for (int b : toFetch) {
            int[] last = runs.isEmpty() ? null : runs.get(runs.size() - 1);
            if (last != null && last[0] + last[1] == b && last[1] < maxRunBlocks) {
                last[1]++;
            } else {
                runs.add(new int[] {b, 1});
            }
        }
        List<Range> requests = new ArrayList<>(runs.size());
        long total = 0;
        for (int[] run : runs) {
            Range r = runRange(run);
            requests.add(r);
            total += r.length();
        }

        CompletableFuture<List<ByteBuffer>> f;
        try {
            f = origin.readv(requests);
        } catch (RuntimeException e) {
            f = CompletableFuture.failedFuture(e);
        }
        f.whenComplete((bufs, err) -> {
            if (err == null && bufs.size() != runs.size()) {
                err = new IOException("origin returned " + bufs.size() + " buffers for "
                        + runs.size() + " ranges");
            }
            if (err != null) {
                fail(toFetch, futures, unwrap(err));
                return;
            }
            for (int k = 0; k < runs.size(); k++) {
                int[] run = runs.get(k);
                Range r = requests.get(k);
                ByteBuffer buf = bufs.get(k);
                if (buf.remaining() != r.length()) {
                    List<Integer> blocks = blocksOf(run);
                    fail(blocks, futures, new EOFException("origin returned " + buf.remaining()
                            + " bytes for " + r + " of " + originUrl));
                    continue;
                }
                deliver(run, buf, futures);
                schedulePublish(run, r, buf.duplicate());
            }
        });
        return total;
    }

    private void deliver(int[] run, ByteBuffer buf, Map<Integer, CompletableFuture<ByteBuffer>> futures) {
        int base = buf.position();
        for (int j = 0; j < run[1]; j++) {
            int off = j * blockSize;
            int len = Math.min(blockSize, buf.remaining() - off);
            futures.get(run[0] + j).complete(buf.slice(base + off, len).asReadOnlyBuffer());
        }
    }

    private void schedulePublish(int[] run, Range r, ByteBuffer buf) {
        try {
            io.execute(() -> publish(run, r, buf));
        } catch (RejectedExecutionException e) {
            discard(blocksOf(run));
        }
    }

    private void publish(int[] run, Range r, ByteBuffer buf) {
        try {
            long pos = r.offset();
            while (buf.hasRemaining()) {
                pos += data.write(buf, pos);
            }
        } catch (IOException e) {
            log.warn("cache write failed for {} at {}: {}", key, r, e.toString());
            discard(blocksOf(run));
            return;
        }
        synchronized (lock) {
            for (int j = 0; j < run[1]; j++) {
                present.set(run[0] + j);
                inflight.remove(run[0] + j);
            }
            dirty = true;
            lock.notifyAll();
        }
        onPublished.accept(r.length());
    }

    /** Drops in-flight entries without marking them present. */
    private void discard(List<Integer> blocks) {
        synchronized (lock) {
            blocks.forEach(inflight::remove);
            lock.notifyAll();
        }
    }

    private void fail(List<Integer> blocks, Map<Integer, CompletableFuture<ByteBuffer>> futures, Throwable err) {
        discard(blocks);
        for (int b : blocks) {
            futures.get(b).completeExceptionally(err);
        }
    }

    private long readHits(List<Range> ranges, ByteBuffer[] out, BitSet hit) throws IOException {
        long hits = 0;
        for (int i = 0; i < ranges.size(); i++) {
            Range r = ranges.get(i);
            long pos = r.offset();
            long end = r.end();
            while (pos < end) {
                int b = blockOf(pos);
                long spanEnd = Math.min(blockEnd(b), end);
                if (!hit.get(b)) {
                    pos = spanEnd;
                    continue;
                }
                for (int nb = b + 1; spanEnd < end && hit.get(nb); nb++) {
                    spanEnd = Math.min(blockEnd(nb), end);
                }
                readFully(out[i].slice((int) (pos - r.offset()), (int) (spanEnd - pos)), pos);
                hits += spanEnd - pos;
                pos = spanEnd;
            }
        }
        return hits;
    }

    private void copyPending(List<Range> ranges, ByteBuffer[] out, Map<Integer, CompletableFuture<ByteBuffer>> pending) {
        for (int i = 0; i < ranges.size(); i++) {
            Range r = ranges.get(i);
            if (r.length() == 0) {
                continue;
            }
            for (int b = blockOf(r.offset()); b <= blockOf(r.end() - 1); b++) {
                CompletableFuture<ByteBuffer> f = pending.get(b);
                if (f == null) {
                    continue;
                }
                ByteBuffer src = f.join();
                long blockStart = (long) b * blockSize;
                long from = Math.max(r.offset(), blockStart);
                long to = Math.min(r.end(), blockStart + src.remaining());
                out[i].put((int) (from - r.offset()), src, (int) (from - blockStart), (int) (to - from));
            }
        }
    }

    private void readFully(ByteBuffer dst, long pos) throws IOException {
        while (dst.hasRemaining()) {
            int n = data.read(dst, pos + dst.position());
            if (n < 0) {
                throw new EOFException("cache data file shorter than expected: " + key);
            }
        }
    }

    /** Writes metadata if it changed. Data is forced first so metadata never runs ahead of it. */
    void flush() throws IOException {
        synchronized (flushLock) {
            FileMeta snapshot;
            synchronized (lock) {
                if (!dirty) {
                    return;
                }
                snapshot = new FileMeta(key, originUrl, fileSize, blockSize, lastAccess, (BitSet) present.clone());
                dirty = false;
            }
            try {
                data.force(false);
                snapshot.write(metaPath);
            } catch (IOException e) {
                synchronized (lock) {
                    dirty = true;
                }
                throw e;
            }
        }
    }

    /** Waits up to {@code timeout} for outstanding fetches, then flushes and closes. */
    void close(Duration timeout) throws IOException {
        synchronized (lock) {
            closed = true;
            long deadline = System.nanoTime() + timeout.toNanos();
            boolean interrupted = false;
            while (!inflight.isEmpty()) {
                long remainingMs = (deadline - System.nanoTime()) / 1_000_000;
                if (remainingMs <= 0) {
                    log.warn("closing {} with {} blocks still in flight", key, inflight.size());
                    break;
                }
                try {
                    lock.wait(remainingMs);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            flush();
        } finally {
            data.close();
        }
    }

    private Range runRange(int[] run) {
        long start = (long) run[0] * blockSize;
        long end = Math.min((long) (run[0] + run[1]) * blockSize, fileSize);
        return new Range(start, (int) (end - start));
    }

    private static List<Integer> blocksOf(int[] run) {
        List<Integer> blocks = new ArrayList<>(run[1]);
        for (int j = 0; j < run[1]; j++) {
            blocks.add(run[0] + j);
        }
        return blocks;
    }

    private int blockOf(long pos) {
        return (int) (pos / blockSize);
    }

    private long blockEnd(int b) {
        return (long) (b + 1) * blockSize;
    }

    private static Throwable unwrap(Throwable t) {
        return t instanceof CompletionException && t.getCause() != null ? t.getCause() : t;
    }
}
