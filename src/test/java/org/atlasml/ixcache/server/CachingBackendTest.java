package org.atlasml.ixcache.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.atlasml.ixcache.cache.Cache;
import org.atlasml.ixcache.cache.CacheConfig;
import org.atlasml.ixcache.cache.Range;
import org.atlasml.ixcache.origin.OriginBackend;
import org.atlasml.ixcache.origin.OriginConfig;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The whole read path over TCP: a client (ixcache's origin client) reads through
 * ixcache's front end and cache, which fetch misses from origin servers (ixcache
 * front ends serving local directories).
 */
class CachingBackendTest {

    static final String SCOPE = "user.test";
    static final String NAME = "f.root";
    static final int SIZE = 3 * 1024 * 1024 + 4321;

    @TempDir
    Path dir;

    byte[] content;
    String rucioPath;
    ExecutorService io;
    XrootdServer originA;
    XrootdServer ixcache;
    Cache cache;
    OriginBackend upstream;
    OriginBackend client;

    @BeforeEach
    void start() throws Exception {
        content = new byte[SIZE];
        new Random(3).nextBytes(content);
        String hex = HexFormat.of().formatHex(MessageDigest.getInstance("MD5")
                .digest((SCOPE + ":" + NAME).getBytes(StandardCharsets.UTF_8)));
        rucioPath = "/disk/rucio/user/test/" + hex.substring(0, 2) + "/" + hex.substring(2, 4) + "/" + NAME;
        Path file = dir.resolve("originA" + rucioPath);
        Files.createDirectories(file.getParent());
        Files.write(file, content);
        Files.write(dir.resolve("originA/plain.bin"), content);
        Files.createDirectories(dir.resolve("ca"));

        io = Executors.newVirtualThreadPerTaskExecutor();
        originA = startServer(new LocalBackend(dir.resolve("originA"), io));
        cache = Cache.open(CacheConfig.defaults(dir.resolve("cache"), 1L << 30).withBlockSize(64 * 1024));
        upstream = new OriginBackend(originConfig());
        ixcache = startServer(new CachingBackend(cache, upstream, io));
        client = new OriginBackend(originConfig());
    }

    @AfterEach
    void stop() throws Exception {
        client.close();
        ixcache.close();
        upstream.close();
        cache.close();
        if (originA != null) {
            originA.close();
        }
        io.close();
    }

    @Test
    void fullAndSparseReadsMatchAndRepeatReadsNeedNoOrigin() throws Exception {
        List<Range> sparse = sparseRanges(new Random(5));
        try (OpenFile f = open(via(originA, rucioPath))) {
            assertEquals(SIZE, f.info().size());
            check(f, List.of(new Range(0, 1000)));
            check(f, sparse);
        }
        assertTrue(cache.usedBytes() > 0);

        originA.close();
        originA = null;
        // Same ranges again: the entry is known, so the origin is not even opened.
        try (OpenFile f = open(via(1, rucioPath))) {
            assertEquals(SIZE, f.info().size());
            check(f, List.of(new Range(0, 1000)));
            check(f, sparse);
            // A block never read before needs the (now unreachable) origin.
            ExecutionException e = assertThrows(ExecutionException.class,
                    () -> f.readv(List.of(new Range(SIZE - 10, 10))).get(30, TimeUnit.SECONDS));
            assertInstanceOf(XrootdException.class, unwrap(e));
        }
    }

    @Test
    void wholeFileReadThenServedFromDisk() throws Exception {
        try (OpenFile f = open(via(originA, rucioPath))) {
            check(f, List.of(new Range(0, SIZE)));
        }
        assertEquals(SIZE, cache.usedBytes());
        try (OpenFile f = open(via(1, rucioPath))) {
            check(f, List.of(new Range(0, SIZE)));
        }
    }

    @Test
    void rucioFileThroughAnotherOriginSharesTheEntry() throws Exception {
        Range r = new Range(100_000, 200_000);
        try (OpenFile f = open(via(originA, rucioPath))) {
            check(f, List.of(r));
        }
        // Another origin and site prefix, same scope:name: served from the shared entry.
        String other = "/pnfs/elsewhere/rucio/" + rucioPath.substring(rucioPath.indexOf("/rucio/") + 7);
        try (OpenFile f = open(via(1, other))) {
            check(f, List.of(r));
        }
    }

    @Test
    void nonRucioFilesAreKeyedByOrigin() throws Exception {
        try (OpenFile f = open(via(originA, "/plain.bin"))) {
            check(f, List.of(new Range(0, 5000)));
        }
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> client.open(via(1, "/plain.bin"), "").get(30, TimeUnit.SECONDS));
        assertInstanceOf(XrootdException.class, unwrap(e));
    }

    @Test
    void readsAtEndOfFile() throws Exception {
        try (OpenFile f = open(via(originA, rucioPath))) {
            check(f, List.of(new Range(SIZE - 100, 100)));
            check(f, List.of(new Range(SIZE - 70_000, 1000), new Range(SIZE - 5, 5)));
            ExecutionException e = assertThrows(ExecutionException.class,
                    () -> f.readv(List.of(new Range(0, 10), new Range(SIZE - 5, 50))).get(30, TimeUnit.SECONDS));
            assertEquals(XrootdProtocol.kXR_FSError, ((XrootdException) unwrap(e)).getError());
        }
    }

    @Test
    void originErrorsReachTheClient() {
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> client.open(via(originA, "/missing.bin"), "").get(30, TimeUnit.SECONDS));
        assertEquals(XrootdProtocol.kXR_NotFound, ((XrootdException) unwrap(e)).getError());
    }

    private void check(OpenFile f, List<Range> ranges) throws Exception {
        List<ByteBuffer> data = f.readv(ranges).get(60, TimeUnit.SECONDS);
        for (int i = 0; i < ranges.size(); i++) {
            Range r = ranges.get(i);
            byte[] want = new byte[r.length()];
            System.arraycopy(content, (int) r.offset(), want, 0, r.length());
            byte[] got = new byte[data.get(i).remaining()];
            data.get(i).duplicate().get(got);
            assertArrayEquals(want, got, "range " + r);
        }
    }

    private static List<Range> sparseRanges(Random rnd) {
        List<Range> out = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            out.add(new Range(rnd.nextInt(SIZE - 20_000), 1 + rnd.nextInt(20_000)));
        }
        return out;
    }

    private OpenFile open(String path) throws Exception {
        return client.open(path, "").get(30, TimeUnit.SECONDS);
    }

    /** The client path for {@code originPath} on {@code origin}, read through ixcache. */
    private String via(XrootdServer origin, String originPath) {
        return via(origin.localAddress().getPort(), originPath);
    }

    private String via(int originPort, String originPath) {
        return "/root://127.0.0.1:" + ixcache.localAddress().getPort()
                + "//root://127.0.0.1:" + originPort + "/" + originPath;
    }

    private XrootdServer startServer(Backend backend) throws Exception {
        return XrootdServer.start(new ServerConfig(0, ServerConfig.Auth.NONE, null, null, dir.resolve("ca"),
                Optional.empty()), backend);
    }

    private OriginConfig originConfig() {
        return new OriginConfig(Optional.empty(), dir.resolve("ca"), Duration.ofSeconds(3),
                Duration.ofSeconds(20), 2, 8, 4, 2, Duration.ofMinutes(1));
    }

    private static Throwable unwrap(Throwable t) {
        while ((t instanceof ExecutionException || t instanceof CompletionException) && t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }
}
