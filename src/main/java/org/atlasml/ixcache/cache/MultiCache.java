package org.atlasml.ixcache.cache;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Random;
import java.util.random.RandomGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache spread over several directories, normally one per disk. Each directory
 * is an independent {@link Cache} with its own index, LRU and watermarks, so a
 * disk can be added, removed or fail without affecting the others.
 *
 * <p>A file stays on the disk it was placed on. A new file goes to a disk chosen
 * at random with probability proportional to the disk's free space below its
 * high watermark: larger and emptier disks get more files, fill levels converge,
 * and new writes are spread over all disks rather than aimed at the emptiest one.
 */
public final class MultiCache implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(MultiCache.class);

    private final List<Cache> disks;
    private final RandomGenerator random;
    /** Disks chosen for files whose first open is still creating the entry. */
    private final Map<String, Cache> placing = new HashMap<>(); // guarded by this

    MultiCache(List<Cache> disks, RandomGenerator random) {
        if (disks.isEmpty()) {
            throw new IllegalArgumentException("no cache directories");
        }
        this.disks = List.copyOf(disks);
        this.random = random;
    }

    /**
     * Opens one {@link Cache} per configuration. A directory that fails to open
     * is logged and left out; at least one must open.
     */
    public static MultiCache open(List<CacheConfig> configs) throws IOException {
        List<Cache> disks = new ArrayList<>();
        for (CacheConfig cfg : configs) {
            try {
                disks.add(Cache.open(cfg));
            } catch (IOException | RuntimeException e) {
                log.error("cache directory {} failed to open, leaving it out: {}", cfg.root(), e.toString());
            }
        }
        if (disks.isEmpty()) {
            throw new IOException("no usable cache directory among " + configs.stream().map(CacheConfig::root).toList());
        }
        return new MultiCache(disks, new Random());
    }

    public List<Cache> disks() {
        return disks;
    }

    /** Size recorded for {@code key} on whichever disk holds it. */
    public OptionalLong knownSize(CacheKey key) {
        Cache c = find(key);
        return c == null ? OptionalLong.empty() : c.knownSize(key);
    }

    /** As {@link Cache#acquire}, on the disk holding {@code key} or, for a new file, a chosen one. */
    public CacheHandle acquire(CacheKey key, String originUrl, long fileSize) throws IOException {
        Cache target;
        boolean placed = false;
        synchronized (this) {
            target = find(key);
            if (target == null) {
                target = placing.get(key.id());
            }
            if (target == null) {
                target = choose();
                placing.put(key.id(), target);
                placed = true;
            }
        }
        try {
            return target.acquire(key, originUrl, fileSize);
        } finally {
            if (placed) {
                synchronized (this) {
                    placing.remove(key.id());
                }
            }
        }
    }

    private Cache find(CacheKey key) {
        for (Cache c : disks) {
            if (c.knownSize(key).isPresent()) {
                return c;
            }
        }
        return null;
    }

    /** A disk for a new file, weighted by free space below the high watermark. */
    Cache choose() {
        long[] weight = new long[disks.size()];
        long total = 0;
        for (int i = 0; i < disks.size(); i++) {
            Cache c = disks.get(i);
            weight[i] = Math.max(0, c.config().highBytes() - c.usedBytes());
            total += weight[i];
        }
        if (total == 0) {
            // Every disk is at its high watermark: fall back to size, eviction makes room.
            for (int i = 0; i < disks.size(); i++) {
                weight[i] = disks.get(i).config().capacityBytes();
                total += weight[i];
            }
        }
        long r = random.nextLong(total);
        for (int i = 0; i < disks.size(); i++) {
            r -= weight[i];
            if (r < 0) {
                return disks.get(i);
            }
        }
        return disks.get(disks.size() - 1);
    }

    /** Bytes cached on all disks. */
    public long usedBytes() {
        return disks.stream().mapToLong(Cache::usedBytes).sum();
    }

    /** Total size of all disks' caches. */
    public long capacityBytes() {
        return disks.stream().mapToLong(c -> c.config().capacityBytes()).sum();
    }

    @Override
    public void close() throws IOException {
        IOException first = null;
        for (Cache c : disks) {
            try {
                c.close();
            } catch (IOException e) {
                if (first == null) {
                    first = e;
                } else {
                    first.addSuppressed(e);
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }
}
