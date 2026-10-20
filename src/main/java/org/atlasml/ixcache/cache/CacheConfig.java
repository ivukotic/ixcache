package org.atlasml.ixcache.cache;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger log = LoggerFactory.getLogger(CacheConfig.class);

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

    /**
     * One configuration per cache directory, from the environment:
     * {@code IXCACHE_CACHE_DIR} (comma-separated directories, typically one per
     * disk), {@code IXCACHE_CACHE_SIZE} (per directory; bytes, or with a K/M/G/T
     * suffix; default: the size of the directory's file system),
     * {@code IXCACHE_HIGH_WM} and {@code IXCACHE_LOW_WM} (fractions of the size),
     * {@code IXCACHE_BLOCK_SIZE}.
     *
     * <p>A directory that cannot be created or inspected is logged and left out.
     * Without {@code IXCACHE_CACHE_SIZE}, two directories on one file system are
     * refused: each would count the whole file system as its own.
     */
    public static List<CacheConfig> fromEnv(Map<String, String> env) throws IOException {
        String size = env.get("IXCACHE_CACHE_SIZE");
        boolean explicitSize = size != null && !size.isBlank();
        double high = Double.parseDouble(env.getOrDefault("IXCACHE_HIGH_WM", "0.95"));
        double low = Double.parseDouble(env.getOrDefault("IXCACHE_LOW_WM", "0.90"));
        int blockSize = (int) parseBytes(env.getOrDefault("IXCACHE_BLOCK_SIZE", String.valueOf(DEFAULT_BLOCK_SIZE)));

        List<CacheConfig> out = new ArrayList<>();
        Map<FileStore, Path> stores = new HashMap<>();
        for (String dir : env.getOrDefault("IXCACHE_CACHE_DIR", "").split(",")) {
            if (dir.isBlank()) {
                continue;
            }
            Path root = Path.of(dir.trim());
            FileStore store;
            try {
                Files.createDirectories(root);
                store = Files.getFileStore(root);
            } catch (IOException e) {
                log.error("cache directory {} is unusable, leaving it out: {}", root, e.toString());
                continue;
            }
            Path other = stores.putIfAbsent(store, root);
            if (other != null && !explicitSize) {
                throw new IllegalArgumentException("cache directories " + other + " and " + root
                        + " are on the same file system; set IXCACHE_CACHE_SIZE or list one of them");
            }
            long capacity = explicitSize ? parseBytes(size) : store.getTotalSpace();
            CacheConfig d = defaults(root, capacity);
            out.add(new CacheConfig(root, capacity, high, low, blockSize,
                    Math.max(d.maxRequestBytes(), blockSize), d.flushInterval(), d.closeTimeout()));
        }
        return out;
    }

    /** Parses {@code 1048576}, {@code 64k}, {@code 500G}, {@code 2T} (powers of 1024). */
    static long parseBytes(String s) {
        String t = s.trim().toUpperCase(Locale.ROOT);
        if (t.endsWith("B")) {
            t = t.substring(0, t.length() - 1);
        }
        int shift = switch (t.isEmpty() ? ' ' : t.charAt(t.length() - 1)) {
            case 'K' -> 10;
            case 'M' -> 20;
            case 'G' -> 30;
            case 'T' -> 40;
            default -> 0;
        };
        String digits = shift == 0 ? t : t.substring(0, t.length() - 1).trim();
        try {
            return Math.multiplyExact(Long.parseLong(digits), 1L << shift);
        } catch (NumberFormatException | ArithmeticException e) {
            throw new IllegalArgumentException("bad size '" + s + "'", e);
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
