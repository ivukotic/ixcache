package org.atlasml.ixcache.server;

import java.io.Closeable;

import org.atlasml.ixcache.cache.OriginReader;

/**
 * A file a client has open. Reads use the {@link OriginReader} contract: one
 * buffer per range, in order. Ranges never extend past {@link FileInfo#size()}.
 */
public interface OpenFile extends OriginReader, Closeable {

    FileInfo info();

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
