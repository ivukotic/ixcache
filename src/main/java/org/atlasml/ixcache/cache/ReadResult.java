package org.atlasml.ixcache.cache;

import java.util.List;

import io.netty.buffer.ByteBuf;

/**
 * Data for a read, one buffer per requested range.
 *
 * @param data         pooled buffers, one per requested range, each holding exactly
 *                     the range; the receiver must release them
 * @param bytesHit     requested bytes served from NVMe
 * @param bytesFetched bytes this read requested from the origin (whole blocks)
 */
public record ReadResult(List<ByteBuf> data, long bytesHit, long bytesFetched) {
}
