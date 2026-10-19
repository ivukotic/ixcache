package org.atlasml.ixcache.cache;

import java.nio.file.Path;
import java.time.Duration;

/**
 * Storage engine settings.
 *
 * @param root            directory holding data and metadata files
 * @param capacityBytes   disk space the cache may use
 * @param highWatermark   fraction of capacity that starts eviction
 * @param lowWatermark    fraction of capacity eviction stops at
 * @param blockSize       block size for newly created entries
 * @param maxRequestBytes largest single range sent to the origin
 * @param flushInterval   how often dirty metadata is written
 * @param closeTimeout    how long closing a file waits for outstanding fetches
 */
public record CacheConfig(
        Path root,
        long capacityBytes,
        double highWatermark,
        double lowWatermark,
        int blockSize,
        int maxRequestBytes,
        Duration flushInterval,
        Duration closeTimeout) {

    public static final int DEFAULT_BLOCK_SIZE = 64 * 1024;

    public CacheConfig {
        if (capacityBytes <= 0) {
            throw new IllegalArgumentException("capacityBytes must be positive");
        }
        if (!(0 < lowWatermark && lowWatermark < highWatermark && highWatermark <= 1)) {
            throw new IllegalArgumentException("need 0 < lowWatermark < highWatermark <= 1");
        }
        if (blockSize < 4096 || Integer.bitCount(blockSize) != 1) {
            throw new IllegalArgumentException("blockSize must be a power of two >= 4096");
        }
        if (maxRequestBytes < blockSize) {
            throw new IllegalArgumentException("maxRequestBytes must be >= blockSize");
        }
    }

    public static CacheConfig defaults(Path root, long capacityBytes) {
        return new CacheConfig(root, capacityBytes, 0.95, 0.90, DEFAULT_BLOCK_SIZE,
                8 * 1024 * 1024, Duration.ofSeconds(30), Duration.ofSeconds(60));
    }

    public CacheConfig withBlockSize(int bs) {
        return new CacheConfig(root, capacityBytes, highWatermark, lowWatermark, bs,
                Math.max(maxRequestBytes, bs), flushInterval, closeTimeout);
    }

    public CacheConfig withMaxRequestBytes(int max) {
        return new CacheConfig(root, capacityBytes, highWatermark, lowWatermark, blockSize,
                max, flushInterval, closeTimeout);
    }

    long highBytes() {
        return (long) (capacityBytes * highWatermark);
    }

    long lowBytes() {
        return (long) (capacityBytes * lowWatermark);
    }
}
