package org.atlasml.ixcache.cache;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/** An open reference to a cached file. Close it when the client closes the file. */
public final class CacheHandle implements Closeable {

    private final Cache cache;
    private final CachedFile file;
    private final Object token;
    private final AtomicBoolean closed = new AtomicBoolean();

    CacheHandle(Cache cache, CachedFile file, Object token) {
        this.cache = cache;
        this.file = file;
        this.token = token;
    }

    public long size() {
        return file.fileSize();
    }

    public int blockSize() {
        return file.blockSize();
    }

    /** Reads one range; misses go to {@code origin}. */
    public CompletableFuture<ReadResult> read(long offset, int length, OriginReader origin) {
        return readv(List.of(new Range(offset, length)), origin);
    }

    /** Reads several ranges; all missing blocks go to {@code origin} in one request. */
    public CompletableFuture<ReadResult> readv(List<Range> ranges, OriginReader origin) {
        if (closed.get()) {
            throw new IllegalStateException("handle closed");
        }
        return file.read(ranges, origin);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            cache.release(token);
        }
    }
}
