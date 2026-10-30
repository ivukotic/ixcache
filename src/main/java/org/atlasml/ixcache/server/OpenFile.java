package org.atlasml.ixcache.server;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.atlasml.ixcache.cache.OriginReader;
import org.atlasml.ixcache.cache.Range;

/**
 * A file a client has open. Reads use the {@link OriginReader} contract: one
 * buffer per range, in order. Ranges never extend past {@link FileInfo#size()}.
 */
public interface OpenFile extends OriginReader, Closeable {

    FileInfo info();

    /**
     * Reads {@code ranges} for sending to a client: as {@link #readv}, but the
     * buffers may be pooled and the caller must release them.
     */
    default CompletableFuture<List<ByteBuf>> read(List<Range> ranges) {
        return readv(ranges).thenApply(bufs -> bufs.stream().map(Unpooled::wrappedBuffer).toList());
    }

    /** Bytes this open has served from local disk (the cache, or a local file). */
    default long bytesFromDisk() {
        return 0;
    }

    /** Bytes this open has had fetched from the origin (whole blocks when caching). */
    default long bytesFromOrigin() {
        return 0;
    }

    @Override
    void close();
}
