package org.atlasml.ixcache.cache;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The cache: an index of cached files on local disk with reference-counted
 * access and file-level LRU eviction between high and low watermarks.
 *
 * <p>Each file lives at {@code root/<h0h1>/<sha256(key)>.data} with its
 * metadata in a {@code .meta} file next to it. Open files cannot be evicted.
 */
public final class Cache implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(Cache.class);

    private final CacheConfig cfg;
    private final ExecutorService io;
    private final ScheduledExecutorService timer;
    private final Map<String, Entry> entries = new HashMap<>(); // guarded by this
    private final AtomicLong used = new AtomicLong();
    private final AtomicBoolean evicting = new AtomicBoolean();
    private boolean closed; // guarded by this

    private static final class Entry {
        final CacheKey key;
        final Path data;
        final Path meta;
        final long fileSize;
        final AtomicLong bytes = new AtomicLong();
        long lastAccess;
        int refs;
        CachedFile file;
        CompletableFuture<Void> closing;

        Entry(CacheKey key, Path data, Path meta, long fileSize, long lastAccess) {
            this.key = key;
            this.data = data;
            this.meta = meta;
            this.fileSize = fileSize;
            this.lastAccess = lastAccess;
        }

        boolean idle() {
            return refs == 0 && file == null && closing == null;
        }
    }

    private Cache(CacheConfig cfg, ExecutorService io) {
        this.cfg = cfg;
        this.io = io;
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ixcache-flush");
            t.setDaemon(true);
            return t;
        });
    }

    /** Opens the cache at {@code cfg.root()}, recovering existing entries. */
    public static Cache open(CacheConfig cfg) throws IOException {
        return open(cfg, Executors.newVirtualThreadPerTaskExecutor());
    }

    /** As {@link #open(CacheConfig)}, with {@code io} running disk writes and closes. */
    public static Cache open(CacheConfig cfg, ExecutorService io) throws IOException {
        Files.createDirectories(cfg.root());
        Cache c = new Cache(cfg, io);
        c.recover();
        long period = cfg.flushInterval().toMillis();
        c.timer.scheduleWithFixedDelay(c::flushOpen, period, period, TimeUnit.MILLISECONDS);
        c.maybeEvict();
        return c;
    }

    /** Size recorded for {@code key}, if the cache knows the file. */
    public synchronized OptionalLong knownSize(CacheKey key) {
        Entry e = entries.get(key.id());
        return e == null ? OptionalLong.empty() : OptionalLong.of(e.fileSize);
    }

    /** Bytes currently cached on disk. */
    public long usedBytes() {
        return used.get();
    }

    public CacheConfig config() {
        return cfg;
    }

    /**
     * Opens {@code key} for reading, creating an empty entry if needed.
     *
     * @param originUrl recorded in metadata for diagnostics
     * @param fileSize  size reported by the origin; must match an existing entry
     */
    public CacheHandle acquire(CacheKey key, String originUrl, long fileSize) throws IOException {
        while (true) {
            CompletableFuture<Void> wait;
            synchronized (this) {
                if (closed) {
                    throw new IOException("cache closed");
                }
                Entry e = entries.get(key.id());
                if (e != null && e.closing != null) {
                    wait = e.closing;
                } else {
                    if (e == null) {
                        e = create(key, originUrl, fileSize);
                    } else if (e.fileSize != fileSize) {
                        throw new IOException("size mismatch for " + key.id() + ": cached "
                                + e.fileSize + ", origin " + fileSize);
                    } else if (e.file == null) {
                        e.file = openExisting(e);
                    }
                    e.refs++;
                    e.lastAccess = System.currentTimeMillis();
                    e.file.touch(e.lastAccess);
                    return new CacheHandle(this, e.file, e);
                }
            }
            wait.exceptionally(t -> null).join();
        }
    }

    /** Called by {@link CacheHandle#close()}. */
    void release(Object token) {
        Entry e = (Entry) token;
        CachedFile f;
        CompletableFuture<Void> closing;
        synchronized (this) {
            if (--e.refs > 0) {
                return;
            }
            f = e.file;
            closing = new CompletableFuture<>();
            e.closing = closing;
        }
        Runnable close = () -> {
            try {
                f.close(cfg.closeTimeout());
            } catch (IOException ex) {
                log.warn("closing {} failed: {}", e.key.id(), ex.toString());
            } finally {
                synchronized (this) {
                    e.file = null;
                    e.closing = null;
                }
                closing.complete(null);
            }
        };
        try {
            io.execute(close);
        } catch (java.util.concurrent.RejectedExecutionException ex) {
            close.run();
        }
    }

    private Entry create(CacheKey key, String originUrl, long fileSize) throws IOException {
        String name = key.fileName();
        Path dir = cfg.root().resolve(name.substring(0, 2));
        Files.createDirectories(dir);
        Entry e = new Entry(key, dir.resolve(name + ".data"), dir.resolve(name + ".meta"),
                fileSize, System.currentTimeMillis());
        Files.deleteIfExists(e.data);
        Files.deleteIfExists(e.meta);
        FileMeta meta = new FileMeta(key.id(), originUrl, fileSize, cfg.blockSize(), e.lastAccess, new BitSet());
        e.file = CachedFile.open(e.data, e.meta, meta, cfg.maxRequestBytes(), io, published(e), true);
        entries.put(key.id(), e);
        return e;
    }

    private CachedFile openExisting(Entry e) throws IOException {
        FileMeta meta = FileMeta.read(e.meta);
        return CachedFile.open(e.data, e.meta, meta, cfg.maxRequestBytes(), io, published(e), false);
    }

    private java.util.function.LongConsumer published(Entry e) {
        return n -> {
            e.bytes.addAndGet(n);
            used.addAndGet(n);
            maybeEvict();
        };
    }

    private void maybeEvict() {
        if (used.get() > cfg.highBytes() && evicting.compareAndSet(false, true)) {
            try {
                io.execute(this::evict);
            } catch (java.util.concurrent.RejectedExecutionException e) {
                evicting.set(false);
            }
        }
    }

    /** Removes least recently used idle files until usage is at or below the low watermark. */
    void evict() {
        try {
            List<Entry> victims = new ArrayList<>();
            synchronized (this) {
                long excess = used.get() - cfg.lowBytes();
                List<Entry> idle = entries.values().stream()
                        .filter(Entry::idle)
                        .sorted(Comparator.comparingLong(e -> e.lastAccess))
                        .toList();
                for (Entry e : idle) {
                    if (excess <= 0) {
                        break;
                    }
                    victims.add(e);
                    entries.remove(e.key.id());
                    excess -= e.bytes.get();
                }
            }
            for (Entry e : victims) {
                try {
                    Files.deleteIfExists(e.meta);
                    Files.deleteIfExists(e.data);
                } catch (IOException ex) {
                    log.warn("evicting {} failed: {}", e.key.id(), ex.toString());
                }
                used.addAndGet(-e.bytes.get());
            }
            if (!victims.isEmpty()) {
                log.info("evicted {} files, cache now uses {} bytes", victims.size(), used.get());
            }
        } finally {
            evicting.set(false);
        }
    }

    private void recover() throws IOException {
        int kept = 0;
        int dropped = 0;
        try (Stream<Path> files = Files.walk(cfg.root(), 2)) {
            for (Path p : files.filter(Files::isRegularFile).filter(Cache::isCacheFile).toList()) {
                String name = p.getFileName().toString();
                if (name.endsWith(".tmp")) {
                    Files.deleteIfExists(p);
                } else if (name.endsWith(".data")) {
                    Path meta = p.resolveSibling(name.substring(0, name.length() - 5) + ".meta");
                    if (!Files.exists(meta)) {
                        Files.deleteIfExists(p);
                        dropped++;
                    }
                } else if (name.endsWith(".meta")) {
                    if (recoverEntry(p)) {
                        kept++;
                    } else {
                        dropped++;
                    }
                }
            }
        }
        log.info("recovered {} cached files ({} bytes), dropped {}", kept, used.get(), dropped);
    }

    private static final Pattern CACHE_FILE = Pattern.compile("([0-9a-f]{2})[0-9a-f]{62}\\.(data|meta|meta\\.tmp)");

    /**
     * True for files the cache creates ({@code <h0h1>/<sha256>.data|.meta|.meta.tmp}).
     * Recovery deletes stray files, so it must never touch anything else that
     * happens to share the directory.
     */
    private static boolean isCacheFile(Path p) {
        Matcher m = CACHE_FILE.matcher(p.getFileName().toString());
        Path parent = p.getParent();
        return m.matches() && parent != null && parent.getFileName().toString().equals(m.group(1));
    }

    private boolean recoverEntry(Path metaPath) throws IOException {
        String name = metaPath.getFileName().toString();
        Path data = metaPath.resolveSibling(name.substring(0, name.length() - 5) + ".data");
        try {
            FileMeta meta = FileMeta.read(metaPath);
            CacheKey key = new CacheKey(meta.key());
            if (!Files.exists(data) || !(key.fileName() + ".meta").equals(name)) {
                throw new IOException("data file missing or name mismatch");
            }
            Entry e = new Entry(key, data, metaPath, meta.fileSize(), meta.lastAccess());
            e.bytes.set(meta.cachedBytes());
            used.addAndGet(e.bytes.get());
            synchronized (this) {
                entries.put(key.id(), e);
            }
            return true;
        } catch (IOException | RuntimeException ex) {
            log.warn("dropping cache entry {}: {}", metaPath, ex.toString());
            Files.deleteIfExists(metaPath);
            Files.deleteIfExists(data);
            return false;
        }
    }

    private void flushOpen() {
        List<CachedFile> open;
        synchronized (this) {
            open = entries.values().stream().map(e -> e.file).filter(f -> f != null).toList();
        }
        for (CachedFile f : open) {
            try {
                f.flush();
            } catch (IOException | RuntimeException e) {
                log.warn("metadata flush failed: {}", e.toString());
            }
        }
    }

    /** Flushes and closes all open files. Open handles become unusable. */
    @Override
    public void close() throws IOException {
        List<CompletableFuture<Void>> pending = new ArrayList<>();
        List<Entry> open = new ArrayList<>();
        synchronized (this) {
            closed = true;
            for (Entry e : entries.values()) {
                if (e.closing != null) {
                    pending.add(e.closing);
                } else if (e.file != null) {
                    open.add(e);
                }
            }
        }
        timer.shutdownNow();
        pending.forEach(f -> f.exceptionally(t -> null).join());
        for (Entry e : open) {
            try {
                e.file.close(cfg.closeTimeout());
            } catch (IOException ex) {
                log.warn("closing {} failed: {}", e.key.id(), ex.toString());
            }
        }
        io.shutdown();
    }
}
