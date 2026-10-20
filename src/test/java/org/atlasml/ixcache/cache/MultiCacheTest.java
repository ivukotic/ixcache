package org.atlasml.ixcache.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MultiCacheTest {

    static final long MB = 1024 * 1024;
    static final long GB = 1024 * MB;

    @TempDir
    Path root;

    List<Cache> opened = new ArrayList<>();

    @AfterEach
    void tearDown() throws IOException {
        for (Cache c : opened) {
            c.close();
        }
    }

    Cache disk(String name, long capacity) throws IOException {
        Cache c = Cache.open(CacheConfig.defaults(root.resolve(name), capacity));
        opened.add(c);
        return c;
    }

    @Test
    void newFilesGoToDisksInProportionToFreeSpace() throws Exception {
        Cache small = disk("small", GB);
        Cache medium = disk("medium", 2 * GB);
        Cache large = disk("large", 4 * GB);
        MultiCache m = new MultiCache(List.of(small, medium, large), new Random(1));

        Map<Cache, Integer> picks = new HashMap<>();
        int n = 70_000;
        for (int i = 0; i < n; i++) {
            picks.merge(m.choose(), 1, Integer::sum);
        }
        assertEquals(1.0 / 7, picks.get(small) / (double) n, 0.01);
        assertEquals(2.0 / 7, picks.get(medium) / (double) n, 0.01);
        assertEquals(4.0 / 7, picks.get(large) / (double) n, 0.01);
    }

    @Test
    void fullerDisksGetFewerNewFiles() throws Exception {
        Cache a = disk("a", 64 * MB);
        Cache b = disk("b", 64 * MB);
        // Fill most of disk a.
        FakeOrigin origin = new FakeOrigin(48 * MB);
        try (CacheHandle h = a.acquire(new CacheKey("rucio:s:big"), "root://o//big", origin.size)) {
            h.read(0, (int) origin.size, origin).join();
        }
        CacheTest.awaitPublished(a, 48 * MB);

        MultiCache m = new MultiCache(List.of(a, b), new Random(2));
        int toA = 0;
        for (int i = 0; i < 10_000; i++) {
            if (m.choose() == a) {
                toA++;
            }
        }
        // free below high watermark: a ~12.8 MB, b ~60.8 MB
        double expected = (0.95 * 64 - 48) / (0.95 * 64 * 2 - 48);
        assertEquals(expected, toA / 10_000.0, 0.02);
    }

    @Test
    void filesStayOnTheirDiskAcrossRestarts() throws Exception {
        List<CacheConfig> cfgs = List.of(
                CacheConfig.defaults(root.resolve("d1"), GB),
                CacheConfig.defaults(root.resolve("d2"), GB),
                CacheConfig.defaults(root.resolve("d3"), GB));
        List<CacheKey> keys = new ArrayList<>();
        try (MultiCache m = MultiCache.open(cfgs)) {
            for (int i = 0; i < 30; i++) {
                CacheKey k = new CacheKey("rucio:s:f" + i);
                keys.add(k);
                m.acquire(k, "root://o//f" + i, 1000 + i).close();
            }
        }
        try (MultiCache m = MultiCache.open(cfgs)) {
            for (int i = 0; i < keys.size(); i++) {
                assertEquals(1000 + i, m.knownSize(keys.get(i)).getAsLong());
                m.acquire(keys.get(i), "root://other//f" + i, 1000 + i).close();
            }
        }
        assertEquals(30, metaFiles().size());
        for (CacheConfig c : cfgs) {
            try (Stream<Path> s = Files.list(c.root())) {
                assertTrue(s.findAny().isPresent(), "nothing placed on " + c.root());
            }
        }
    }

    @Test
    void concurrentFirstOpensOfOneFileUseOneDisk() throws Exception {
        List<Cache> disks = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            disks.add(disk("d" + i, GB));
        }
        MultiCache m = new MultiCache(disks, new Random(3));
        CacheKey key = new CacheKey("rucio:s:hot");
        ExecutorService pool = Executors.newFixedThreadPool(16);
        try {
            List<Callable<CacheHandle>> tasks = new ArrayList<>();
            for (int i = 0; i < 64; i++) {
                tasks.add(() -> m.acquire(key, "root://o//hot", 12345));
            }
            for (Future<CacheHandle> f : pool.invokeAll(tasks)) {
                f.get().close();
            }
        } finally {
            pool.shutdown();
        }
        assertEquals(1, metaFiles().size());
    }

    @Test
    void openSkipsUnusableDirectories() throws Exception {
        Path notADir = Files.writeString(root.resolve("file"), "x");
        List<CacheConfig> cfgs = List.of(
                CacheConfig.defaults(notADir, GB),
                CacheConfig.defaults(root.resolve("ok"), GB));
        try (MultiCache m = MultiCache.open(cfgs)) {
            assertEquals(1, m.disks().size());
            assertEquals(root.resolve("ok"), m.disks().get(0).config().root());
        }
        assertThrows(IOException.class, () -> MultiCache.open(List.of(CacheConfig.defaults(notADir, GB))));
    }

    private List<Path> metaFiles() throws IOException {
        try (Stream<Path> s = Files.walk(root)) {
            return s.filter(p -> p.toString().endsWith(".meta")).toList();
        }
    }
}
