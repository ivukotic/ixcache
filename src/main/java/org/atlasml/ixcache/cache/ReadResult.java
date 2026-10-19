package org.atlasml.ixcache.cache;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Data for a read, one buffer per requested range.
 *
 * @param data         buffers positioned at 0, one per requested range
 * @param bytesHit     requested bytes served from NVMe
 * @param bytesFetched bytes this read requested from the origin (whole blocks)
 */
public record ReadResult(List<ByteBuffer> data, long bytesHit, long bytesFetched) {
}
