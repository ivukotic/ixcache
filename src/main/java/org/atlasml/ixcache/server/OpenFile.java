package org.atlasml.ixcache.server;

import java.io.Closeable;

import org.atlasml.ixcache.cache.OriginReader;

/**
 * A file a client has open. Reads use the {@link OriginReader} contract: one
 * buffer per range, in order. Ranges never extend past {@link FileInfo#size()}.
 */
public interface OpenFile extends OriginReader, Closeable {

    FileInfo info();

    @Override
    void close();
}
