package org.atlasml.ixcache.server;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.atlasml.ixcache.cache.Cache;
import org.atlasml.ixcache.cache.CacheHandle;
import org.atlasml.ixcache.cache.CacheKey;
import org.atlasml.ixcache.cache.Range;
import org.atlasml.ixcache.path.RequestPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serves reads from the cache, fetching missing blocks through {@code origin}.
 *
 * <p>A file the cache already knows is opened without contacting the origin: the
 * origin file is opened on the first miss, so a fully cached file never touches
 * it. A new file is opened at the origin first, to learn its size. Files are
 * keyed by {@link RequestPath#cacheKey()}, so the same Rucio file read through
 * different origins shares one entry; misses always go to the origin the client
 * named.
 */
public final class CachingBackend implements Backend {

    private static final Logger log = LoggerFactory.getLogger(CachingBackend.class);

    private final Cache cache;
    private final Backend origin;
    private final Executor blocking;

    /**
     * @param blocking runs {@link Cache#acquire}, which may wait for a closing entry
     */
    public CachingBackend(Cache cache, Backend origin, Executor blocking) {
        this.cache = cache;
        this.origin = origin;
        this.blocking = blocking;
    }

    @Override
    public CompletableFuture<OpenFile> open(String path, String opaque) {
        RequestPath rp;
        try {
            rp = RequestPath.parse(path);
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(e);
        }
        CacheKey key = rp.cacheKey();
        Supplier<CompletableFuture<OpenFile>> openOrigin = () -> origin.open(path, opaque);

        OptionalLong known = cache.knownSize(key);
        if (known.isPresent()) {
            long size = known.getAsLong();
            return CompletableFuture.supplyAsync(() -> acquire(key, rp, size), blocking)
                    .thenApply(h -> new CachedOpenFile(key, h, new FileInfo(size, 0), openOrigin, null));
        }
        return openOrigin.get().thenApplyAsync(of -> {
            try {
                CacheHandle h = acquire(key, rp, of.info().size());
                return new CachedOpenFile(key, h, of.info(), openOrigin, of);
            } catch (RuntimeException e) {
                of.close();
                throw e;
            }
        }, blocking);
    }

    @Override
    public CompletableFuture<FileInfo> stat(String path, String opaque) {
        try {
            OptionalLong known = cache.knownSize(RequestPath.parse(path).cacheKey());
            if (known.isPresent()) {
                return CompletableFuture.completedFuture(new FileInfo(known.getAsLong(), 0));
            }
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(e);
        }
        return origin.stat(path, opaque);
    }

    private CacheHandle acquire(CacheKey key, RequestPath rp, long size) {
        try {
            return cache.acquire(key, rp.originUrl(), size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** One client open of a cached file. */
    private static final class CachedOpenFile implements OpenFile {

        private final CacheKey key;
        private final CacheHandle handle;
        private final FileInfo info;
        private final Supplier<CompletableFuture<OpenFile>> openOrigin;
        private CompletableFuture<OpenFile> originFile; // guarded by this
        private boolean closed; // guarded by this
        private final AtomicLong bytesHit = new AtomicLong();
        private final AtomicLong bytesFetched = new AtomicLong();
        private final AtomicLong bytesOut = new AtomicLong();

        CachedOpenFile(CacheKey key, CacheHandle handle, FileInfo info,
                Supplier<CompletableFuture<OpenFile>> openOrigin, OpenFile alreadyOpen) {
            this.key = key;
            this.handle = handle;
            this.info = info;
            this.openOrigin = openOrigin;
            this.originFile = alreadyOpen == null ? null : CompletableFuture.completedFuture(alreadyOpen);
        }

        @Override
        public FileInfo info() {
            return info;
        }

        @Override
        public CompletableFuture<List<ByteBuffer>> readv(List<Range> ranges) {
            return handle.readv(ranges, missing -> origin().thenCompose(f -> f.readv(missing)))
                    .thenApply(r -> {
                        bytesHit.addAndGet(r.bytesHit());
                        bytesFetched.addAndGet(r.bytesFetched());
                        r.data().forEach(b -> bytesOut.addAndGet(b.remaining()));
                        return r.data();
                    });
        }

        /** The origin file, opened on first use; a failed open is retried by the next miss. */
        private synchronized CompletableFuture<OpenFile> origin() {
            if (closed) {
                return CompletableFuture.failedFuture(new IOException("file closed"));
            }
            if (originFile == null || originFile.isCompletedExceptionally()) {
                originFile = openOrigin.get().thenApply(f -> {
                    if (f.info().size() != handle.size()) {
                        f.close();
                        throw new CompletionException(new IOException("origin size " + f.info().size()
                                + " differs from cached size " + handle.size() + " for " + key.id()));
                    }
                    return f;
                });
            }
            return originFile;
        }

        @Override
        public void close() {
            CompletableFuture<OpenFile> f;
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                f = originFile;
            }
            handle.close();
            if (f != null) {
                f.thenAccept(OpenFile::close);
            }
            log.debug("closed {}: {} bytes out, {} from disk, {} fetched", key.id(),
                    bytesOut.get(), bytesHit.get(), bytesFetched.get());
        }
    }
}
