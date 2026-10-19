package org.atlasml.ixcache.cache;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Reads byte ranges of one open file from its origin. */
public interface OriginReader {

    /**
     * Reads {@code ranges} in a single origin operation where possible (a plain
     * read for one range, a vector read for several). The result has one buffer
     * per range, in order, each positioned at 0 with exactly the range's length
     * remaining. Implementations split requests that exceed protocol limits.
     */
    CompletableFuture<List<ByteBuffer>> readv(List<Range> ranges);
}
