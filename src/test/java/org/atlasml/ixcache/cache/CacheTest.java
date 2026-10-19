package org.atlasml.ixcache.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CacheTest {

    static final int BS = 64 * 1024;
    static final long MB = 1024 * 1024;

    @TempDir
    Path root;

    Cache cache;

    @AfterEach
    void tearDown() throws IOException {
        if (cache != null) {
            cache.close();
        }
    }

    Cache open(CacheConfig cfg) throws IOException {
        cache = Cache.open(cfg);
        return cache;
    }

    CacheConfig cfg() {
        return CacheConfig.defaults(root, 1024 * MB);
    }

    static void assertData(long offset, int length, ByteBuffer actual) {
        assertEquals(FakeOrigin.expected(offset, length), actual, "data at " + offset + "+" + length);
    }

    /** Waits until all scheduled disk writes for {@code h} have been published. */
    static void awaitPublished(Cache c, long bytes) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (c.usedBytes() < bytes && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(bytes, c.usedBytes());
    }

    @Test
    void sparseReadFetchesOnlyCoveringBlocks() throws Exception {
        Cache c = open(cfg());
        FakeOrigin origin = new FakeOrigin(100 * MB);
        try (CacheHandle h = c.acquire(new CacheKey("k"), "root://o//f", origin.size)) {
            ReadResult r = h.read(10 * MB + 100, 1000, origin).join();
            assertData(10 * MB + 100, 1000, r.data().get(0));
            assertEquals(List.of(List.of(new Range(10 * MB, BS))), origin.requests);
            assertEquals(0, r.bytesHit());
            assertEquals(BS, r.bytesFetched());
        }
    }

    @Test
    void secondReadIsServedFromDisk() throws Exception {
        Cache c = open(cfg());
        FakeOrigin origin = new FakeOrigin(10 * MB);
        try (CacheHandle h = c.acquire(new CacheKey("k"), "root://o//f", origin.size)) {
            h.read(1000, 300_000, origin).join();
            awaitPublished(c, 5L * BS);
            int before = origin.requests.size();
            ReadResult r = h.read(5000, 200_000, origin).join();
            assertData(5000, 200_000, r.data().get(0));
            assertEquals(before, origin.requests.size());
            assertEquals(200_000, r.bytesHit());
        }
    }

    @Test
    void partialHitFetchesOnlyMissingBlocks() throws Exception {
        Cache c = open(cfg());
        FakeOrigin origin = new FakeOrigin(10 * MB);
        try (CacheHandle h = c.acquire(new CacheKey("k"), "root://o//f", origin.size)) {
            h.read(BS, BS, origin).join(); // block 1
            awaitPublished(c, BS);
            origin.requests.clear();
            ReadResult r = h.read(0, 3 * BS, origin).join(); // blocks 0..2, 1 is cached
            assertData(0, 3 * BS, r.data().get(0));
            assertEquals(List.of(List.of(new Range(0, BS), new Range(2L * BS, BS))), origin.requests);
            assertEquals(BS, r.bytesHit());
        }
    }

    @Test
    void lastPartialBlockAndReadsPastEof() throws Exception {
        Cache c = open(cfg());
        long size = 3L * BS + 123;
        FakeOrigin origin = new FakeOrigin(size);
        try (CacheHandle h = c.acquire(new CacheKey("k"), "root://o//f", size)) {
            ReadResult r = h.read(3L * BS, 1000, origin).join();
            assertEquals(123, r.data().get(0).remaining());
            assertData(3L * BS, 123, r.data().get(0));
            assertEquals(List.of(List.of(new Range(3L * BS, 123))), origin.requests);
            ReadResult past = h.read(size + 10, 100, origin).join();
            assertEquals(0, past.data().get(0).remaining());
            awaitPublished(c, 123);
        }
    }

    @Test
    void vectorReadBecomesOneOriginRequest() throws Exception {
        Cache c = open(cfg());
        FakeOrigin origin = new FakeOrigin(100 * MB);
        List<Range> ranges = List.of(new Range(0, 10), new Range(5 * MB, 70_000), new Range(50 * MB + 7, 3));
        try (CacheHandle h = c.acquire(new CacheKey("k"), "root://o//f", origin.size)) {
            ReadResult r = h.readv(ranges, origin).join();
            for (int i = 0; i < ranges.size(); i++) {
                assertData(ranges.get(i).offset(), ranges.get(i).length(), r.data().get(i));
            }
            assertEquals(1, origin.requests.size());
            assertEquals(List.of(new Range(0, BS), new Range(5 * MB, 2 * BS), new Range(50 * MB, BS)),
                    origin.requests.get(0));
        }
    }

    @Test
    void largeRunsAreSplitAtMaxRequestSize() throws Exception {
        Cache c = open(cfg().withMaxRequestBytes(4 * BS));
        FakeOrigin origin = new FakeOrigin(100 * MB);
        try (CacheHandle h = c.acquire(new CacheKey("k"), "root://o//f", origin.size)) {
            ReadResult r = h.read(0, 10 * BS, origin).join();
            assertData(0, 10 * BS, r.data().get(0));
            assertEquals(List.of(List.of(new Range(0, 4 * BS), new Range(4L * BS, 4 * BS),
                    new Range(8L * BS, 2 * BS))), origin.requests);
        }
    }

    @Test
    void concurrentOverlappingMissesFetchEachBlockOnce() throws Exception {
        Cache c = open(cfg());
        FakeOrigin origin = new FakeOrigin(100 * MB);
        CompletableFuture<Void> gate = new CompletableFuture<>();
        origin.gate = gate;
        try (CacheHandle h = c.acquire(new CacheKey("k"), "root://o//f", origin.size);
                ExecutorService pool = Executors.newFixedThreadPool(16)) {
            List<CompletableFuture<ReadResult>> results = new ArrayList<>();
            List<Range> asked = new ArrayList<>();
            for (int i = 0; i < 64; i++) {
                Range r = new Range((long) (i % 8) * 50_000, 200_000);
                asked.add(r);
                results.add(CompletableFuture.supplyAsync(() -> h.read(r.offset(), r.length(), origin), pool)
                        .thenCompose(f -> f));
            }
            Thread.sleep(200);
            gate.complete(null);
            for (int i = 0; i < results.size(); i++) {
                assertData(asked.get(i).offset(), asked.get(i).length(), results.get(i).join().data().get(0));
            }
            // blocks covering [0, 550000) = blocks 0..8, each requested exactly once
            long blocks = origin.requests.stream().flatMap(List::stream)
                    .mapToLong(r -> r.length() / BS).sum();
            assertEquals(9, blocks);
            assertEquals(9L * BS, origin.bytesRequested());
        }
    }

    @Test
    void originFailureIsReportedAndRetried() throws Exception {
        Cache c = open(cfg());
        FakeOrigin origin = new FakeOrigin(10 * MB);
        origin.failNext = true;
        try (CacheHandle h = c.acquire(new CacheKey("k"), "root://o//f", origin.size)) {
            CompletionException e = assertThrows(CompletionException.class, () -> h.read(0, 100, origin).join());
            assertTrue(e.getCause() instanceof IOException);
            ReadResult r = h.read(0, 100, origin).join();
            assertData(0, 100, r.data().get(0));
            assertEquals(2, origin.requests.size());
        }
    }

    @Test
    void shortOriginReplyFailsRead() throws Exception {
        Cache c = open(cfg());
        FakeOrigin origin = new FakeOrigin(BS / 2); // origin file shorter than the cache believes
        try (CacheHandle h = c.acquire(new CacheKey("k"), "root://o//f", 2L * BS)) {
            CompletionException e = assertThrows(CompletionException.class, () -> h.read(0, 100, origin).join());
            assertTrue(e.getCause() instanceof java.io.EOFException);
        }
    }

    @Test
    void entriesSurviveRestart() throws Exception {
        Cache c = open(cfg());
        FakeOrigin origin = new FakeOrigin(10 * MB);
        CacheKey key = new CacheKey("rucio:user.x:f");
        try (CacheHandle h = c.acquire(key, "root://o//f", origin.size)) {
            h.read(0, 3 * BS, origin).join();
            awaitPublished(c, 3L * BS);
        }
        c.close();
        cache = null;

        Cache c2 = open(cfg());
        assertEquals(3L * BS, c2.usedBytes());
        assertEquals(10 * MB, c2.knownSize(key).getAsLong());
        FakeOrigin origin2 = new FakeOrigin(10 * MB);
        try (CacheHandle h = c2.acquire(key, "root://other//f", origin2.size)) {
            ReadResult r = h.read(100, 2 * BS, origin2).join();
            assertData(100, 2 * BS, r.data().get(0));
            assertTrue(origin2.requests.isEmpty());
        }
    }

    @Test
    void unflushedBlocksAreNotTrustedAfterCrash() throws Exception {
        // simulate a crash: data written, metadata never flushed after the read
        CacheConfig cfg = new CacheConfig(root, 1024 * MB, 0.95, 0.9, BS, 8 * (int) MB,
                java.time.Duration.ofHours(1), java.time.Duration.ofSeconds(5));
        Cache c = open(cfg);
        FakeOrigin origin = new FakeOrigin(10 * MB);
        CacheKey key = new CacheKey("k");
        CacheHandle h = c.acquire(key, "root://o//f", origin.size);
        h.read(0, BS, origin).join();
        awaitPublished(c, BS);
        cache = null; // abandon without close

        Cache c2 = Cache.open(cfg);
        try {
            assertEquals(0, c2.usedBytes());
            FakeOrigin origin2 = new FakeOrigin(10 * MB);
            try (CacheHandle h2 = c2.acquire(key, "root://o//f", origin2.size)) {
                assertData(0, BS, h2.read(0, BS, origin2).join().data().get(0));
                assertEquals(1, origin2.requests.size());
            }
        } finally {
            c2.close();
            c.close();
        }
    }

    @Test
    void corruptMetadataIsDropped() throws Exception {
        Cache c = open(cfg());
        FakeOrigin origin = new FakeOrigin(MB);
        try (CacheHandle h = c.acquire(new CacheKey("k"), "root://o//f", origin.size)) {
            h.read(0, BS, origin).join();
            awaitPublished(c, BS);
        }
        c.close();
        cache = null;
        Path meta;
        try (Stream<Path> s = Files.walk(root)) {
            meta = s.filter(p -> p.toString().endsWith(".meta")).findFirst().orElseThrow();
        }
        byte[] b = Files.readAllBytes(meta);
        b[20] ^= 1;
        Files.write(meta, b);

        Cache c2 = open(cfg());
        assertEquals(0, c2.usedBytes());
        assertTrue(c2.knownSize(new CacheKey("k")).isEmpty());
        try (Stream<Path> s = Files.walk(root)) {
            assertFalse(s.anyMatch(Files::isRegularFile));
        }
    }

    @Test
    void sizeMismatchIsRejected() throws Exception {
        Cache c = open(cfg());
        try (CacheHandle h = c.acquire(new CacheKey("k"), "root://o//f", 1000)) {
            assertThrows(IOException.class, () -> c.acquire(new CacheKey("k"), "root://o//f", 2000));
        }
    }

    @Test
    void evictsLeastRecentlyUsedIdleFilesDownToLowWatermark() throws Exception {
        // capacity 10 blocks: high 9, low 5
        CacheConfig cfg = new CacheConfig(root, 10L * BS, 0.9, 0.5, BS, 8 * (int) MB,
                java.time.Duration.ofSeconds(30), java.time.Duration.ofSeconds(5));
        Cache c = open(cfg);
        for (int i = 0; i < 3; i++) {
            FakeOrigin origin = new FakeOrigin(10L * BS);
            try (CacheHandle h = c.acquire(new CacheKey("f" + i), "root://o//f" + i, origin.size)) {
                h.read(0, 3 * BS, origin).join();
                awaitPublished(c, (i + 1) * 3L * BS);
            }
            Thread.sleep(5); // distinct access times
        }
        // f3 is held open and pushes usage to 12 blocks
        FakeOrigin origin = new FakeOrigin(10L * BS);
        try (CacheHandle h = c.acquire(new CacheKey("f3"), "root://o//f3", origin.size)) {
            h.read(0, 3 * BS, origin).join();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (c.usedBytes() > 5L * BS && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            assertEquals(3L * BS, c.usedBytes());
            assertTrue(c.knownSize(new CacheKey("f0")).isEmpty());
            assertTrue(c.knownSize(new CacheKey("f1")).isEmpty());
            assertTrue(c.knownSize(new CacheKey("f2")).isEmpty());
            assertTrue(c.knownSize(new CacheKey("f3")).isPresent());
        }
    }
}
