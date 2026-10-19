package org.atlasml.ixcache.origin;

import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_IOError;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import io.netty.buffer.ByteBuf;
import org.atlasml.ixcache.cache.Range;
import org.atlasml.ixcache.origin.OriginBackend.Handle;
import org.atlasml.ixcache.origin.OriginBackend.Target;
import org.atlasml.ixcache.origin.OriginConnection.ConnectionLostException;
import org.atlasml.ixcache.server.FileInfo;
import org.atlasml.ixcache.server.OpenFile;
import org.dcache.xrootd.core.XrootdException;

/**
 * A file open at an origin. One range becomes plain reads; several become
 * vector reads, split to respect the protocol's element-size and element-count
 * limits. If the connection is lost, the file is reopened once (from the
 * original URL, so a redirector can pick another server) and the read retried.
 */
final class OriginFile implements OpenFile {

    /** Largest single kXR_read issued; longer ranges are read in parallel pieces. */
    static final int MAX_READ = 8 * 1024 * 1024;
    /** Largest total size of one kXR_readv. */
    static final int MAX_READV_BYTES = 16 * 1024 * 1024;

    private final OriginBackend backend;
    private final Target target;
    private final FileInfo info;
    private Handle handle; // guarded by this
    private CompletableFuture<Handle> reopening; // guarded by this
    private boolean closed; // guarded by this

    OriginFile(OriginBackend backend, Target target, FileInfo info, Handle handle) {
        this.backend = backend;
        this.target = target;
        this.info = info;
        this.handle = handle;
    }

    @Override
    public FileInfo info() {
        return info;
    }

    @Override
    public CompletableFuture<List<ByteBuffer>> readv(List<Range> ranges) {
        Handle h;
        synchronized (this) {
            h = handle;
        }
        CompletableFuture<Handle> current = h.conn().isActive()
                ? CompletableFuture.completedFuture(h) : reopen(h);
        return current.thenCompose(x -> read(x, ranges)).exceptionallyCompose(e -> {
            if (OriginConnection.unwrap(e) instanceof ConnectionLostException) {
                return reopen(h).thenCompose(x -> read(x, ranges));
            }
            return CompletableFuture.failedFuture(OriginConnection.unwrap(e));
        });
    }

    /** Replaces {@code failed} with a fresh open, once, however many reads notice it. */
    private synchronized CompletableFuture<Handle> reopen(Handle failed) {
        if (closed) {
            return CompletableFuture.failedFuture(new XrootdException(kXR_IOError, "file closed"));
        }
        if (handle != failed) {
            return CompletableFuture.completedFuture(handle);
        }
        if (reopening == null) {
            reopening = backend.open(target, 0).thenApply(opened -> {
                synchronized (this) {
                    reopening = null;
                    if (closed) {
                        opened.handle().release();
                        throw new IllegalStateException("file closed during reopen");
                    }
                    if (opened.info().size() != info.size()) {
                        opened.handle().release();
                        throw new IllegalStateException("origin file changed size on reopen: "
                                + info.size() + " -> " + opened.info().size());
                    }
                    handle.release();
                    handle = opened.handle();
                    return handle;
                }
            });
            reopening.whenComplete((v, e) -> {
                if (e != null) {
                    synchronized (this) {
                        reopening = null;
                    }
                }
            });
        }
        return reopening;
    }

    private static CompletableFuture<List<ByteBuffer>> read(Handle h, List<Range> ranges) {
        List<ByteBuffer> out = new ArrayList<>(ranges.size());
        for (Range r : ranges) {
            out.add(ByteBuffer.allocate(r.length()));
        }
        List<CompletableFuture<Void>> ops = new ArrayList<>();
        if (ranges.size() == 1) {
            Range r = ranges.get(0);
            for (int pos = 0; pos < r.length(); pos += MAX_READ) {
                int at = pos;
                int len = Math.min(MAX_READ, r.length() - pos);
                ops.add(h.conn().send(Requests.read(h.fh(), r.offset() + at, len)).thenAccept(reply -> {
                    try {
                        if (reply.data().readableBytes() != len) {
                            throw ioError("short read from origin " + h.conn() + ": "
                                    + reply.data().readableBytes() + " of " + len + " bytes at " + (r.offset() + at));
                        }
                        copy(reply.data(), reply.data().readerIndex(), out.get(0), at, len);
                    } finally {
                        reply.data().release();
                    }
                }));
            }
        } else {
            for (List<Piece> batch : batches(ranges)) {
                List<Range> wire = batch.stream().map(Piece::wire).toList();
                ops.add(h.conn().send(Requests.readv(h.fh(), wire)).thenAccept(reply -> {
                    try {
                        scatter(h, reply.data(), batch, out);
                    } finally {
                        reply.data().release();
                    }
                }));
            }
        }
        return CompletableFuture.allOf(ops.toArray(CompletableFuture[]::new)).thenApply(v -> out);
    }

    /** A readv element: part of request range {@code index}, landing at {@code pos} in its buffer. */
    private record Piece(int index, int pos, Range wire) {
    }

    /** Splits ranges into readv elements and groups them into requests within protocol limits. */
    private static List<List<Piece>> batches(List<Range> ranges) {
        List<List<Piece>> out = new ArrayList<>();
        List<Piece> cur = new ArrayList<>();
        long curBytes = 0;
        for (int i = 0; i < ranges.size(); i++) {
            Range r = ranges.get(i);
            for (int pos = 0; pos < r.length(); pos += Requests.READV_IOR_MAX) {
                int len = Math.min(Requests.READV_IOR_MAX, r.length() - pos);
                if (cur.size() == Requests.READV_IOV_MAX || curBytes + len > MAX_READV_BYTES) {
                    out.add(cur);
                    cur = new ArrayList<>();
                    curBytes = 0;
                }
                cur.add(new Piece(i, pos, new Range(r.offset() + pos, len)));
                curBytes += len;
            }
        }
        if (!cur.isEmpty()) {
            out.add(cur);
        }
        return out;
    }

    /** Copies a readv reply (16-byte header per element, then its data) into the buffers. */
    private static void scatter(Handle h, ByteBuf data, List<Piece> batch, List<ByteBuffer> out) {
        int idx = data.readerIndex();
        for (Piece p : batch) {
            if (data.writerIndex() - idx < 16) {
                throw ioError("truncated readv reply from origin " + h.conn());
            }
            int rlen = data.getInt(idx + 4);
            long offset = data.getLong(idx + 8);
            if (offset != p.wire().offset() || rlen != p.wire().length()
                    || data.writerIndex() - idx - 16 < rlen) {
                throw ioError("readv reply from origin " + h.conn() + " does not match request: got "
                        + rlen + "@" + offset + ", wanted " + p.wire().length() + "@" + p.wire().offset());
            }
            copy(data, idx + 16, out.get(p.index()), p.pos(), rlen);
            idx += 16 + rlen;
        }
    }

    /** An I/O error raised inside a completion stage (reaches the client as kXR_IOError). */
    private static CompletionException ioError(String message) {
        return new CompletionException(new IOException(message));
    }

    private static void copy(ByteBuf src, int srcIndex, ByteBuffer dst, int dstPos, int len) {
        ByteBuffer target = dst.duplicate();
        target.limit(dstPos + len).position(dstPos);
        src.getBytes(srcIndex, target);
    }

    @Override
    public void close() {
        Handle h;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            h = handle;
        }
        h.release();
    }
}
