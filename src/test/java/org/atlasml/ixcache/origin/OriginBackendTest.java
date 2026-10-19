package org.atlasml.ixcache.origin;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.atlasml.ixcache.cache.Range;
import org.atlasml.ixcache.server.FileInfo;
import org.atlasml.ixcache.server.LocalBackend;
import org.atlasml.ixcache.server.OpenFile;
import org.atlasml.ixcache.server.ServerConfig;
import org.atlasml.ixcache.server.XrootdServer;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The origin client against a real XRootD server over TCP: ixcache's own front
 * end serving a local directory, without authentication.
 */
class OriginBackendTest {

    @TempDir
    static Path dir;

    static byte[] content;
    static ExecutorService io;
    static XrootdServer origin;
    static OriginBackend backend;
    static String url;

    @BeforeAll
    static void start() throws Exception {
        content = new byte[20 * 1024 * 1024 + 123];
        new Random(7).nextBytes(content);
        Files.createDirectories(dir.resolve("export/data"));
        Files.write(dir.resolve("export/data/f.bin"), content);
        Files.createDirectories(dir.resolve("ca"));

        io = Executors.newVirtualThreadPerTaskExecutor();
        origin = XrootdServer.start(new ServerConfig(0, ServerConfig.Auth.NONE, null, null, dir.resolve("ca"),
                Optional.empty()), new LocalBackend(dir.resolve("export"), io));
        backend = new OriginBackend(new OriginConfig(Optional.empty(), dir.resolve("ca"),
                Duration.ofSeconds(5), Duration.ofSeconds(20), 2, 8, 4, 2, Duration.ofMinutes(1)));
        url = "/root://127.0.0.1:" + origin.localAddress().getPort() + "//data/f.bin";
    }

    @AfterAll
    static void stop() {
        backend.close();
        origin.close();
        io.close();
    }

    @Test
    void readsWholeFileInPieces() throws Exception {
        try (OpenFile f = backend.open(url, "").get(30, TimeUnit.SECONDS)) {
            assertEquals(content.length, f.info().size());
            List<ByteBuffer> data = f.readv(List.of(new Range(0, content.length))).get(30, TimeUnit.SECONDS);
            assertArrayEquals(content, bytes(data.get(0)));
        }
    }

    @Test
    void vectorReadsSplitLargeElementsAndManyRanges() throws Exception {
        Random rnd = new Random(1);
        List<Range> ranges = new ArrayList<>();
        ranges.add(new Range(1000, 5 * 1024 * 1024)); // larger than one readv element
        for (int i = 0; i < 1500; i++) {             // more elements than one readv carries
            ranges.add(new Range(rnd.nextInt(content.length - 10_000), 1 + rnd.nextInt(9_000)));
        }
        ranges.add(new Range(content.length - 10, 10));
        ranges.add(new Range(5, 0));
        try (OpenFile f = backend.open(url, "").get(30, TimeUnit.SECONDS)) {
            List<ByteBuffer> data = f.readv(ranges).get(60, TimeUnit.SECONDS);
            for (int i = 0; i < ranges.size(); i++) {
                Range r = ranges.get(i);
                byte[] want = new byte[r.length()];
                System.arraycopy(content, (int) r.offset(), want, 0, r.length());
                assertArrayEquals(want, bytes(data.get(i)), "range " + i + " " + r);
            }
        }
    }

    @Test
    void concurrentReadsShareConnections() throws Exception {
        Random rnd = new Random(2);
        List<OpenFile> files = new ArrayList<>();
        List<CompletableFuture<Void>> reads = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            files.add(backend.open(url, "").get(30, TimeUnit.SECONDS));
        }
        for (int i = 0; i < 200; i++) {
            OpenFile f = files.get(i % files.size());
            Range r = new Range(rnd.nextInt(content.length - 100_000), 1 + rnd.nextInt(99_000));
            reads.add(f.readv(List.of(r)).thenAccept(d -> {
                byte[] want = new byte[r.length()];
                System.arraycopy(content, (int) r.offset(), want, 0, r.length());
                assertArrayEquals(want, bytes(d.get(0)));
            }));
        }
        CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new)).get(60, TimeUnit.SECONDS);
        files.forEach(OpenFile::close);
    }

    @Test
    void stat() throws Exception {
        FileInfo info = backend.stat(url, "").get(30, TimeUnit.SECONDS);
        assertEquals(content.length, info.size());
    }

    @Test
    void originErrorsKeepTheirCode() {
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> backend.open(url.replace("f.bin", "missing.bin"), "").get(30, TimeUnit.SECONDS));
        XrootdException x = assertInstanceOf(XrootdException.class, OriginConnection.unwrap(e.getCause()));
        assertEquals(XrootdProtocol.kXR_NotFound, x.getError());
    }

    @Test
    void unreachableOriginFails() {
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> backend.open("/root://127.0.0.1:1//data/f.bin", "").get(30, TimeUnit.SECONDS));
        assertInstanceOf(java.io.IOException.class, OriginConnection.unwrap(e.getCause()));
    }

    @Test
    void forwardsOnlyOpaqueMeantForTheOrigin() {
        assertEquals("authz=abc&eos.app=x", OriginBackend.forwardable(
                "authz=abc&xrdcl.requuid=1&xrd.wantprot=gsi&tried=h1&triedrc=enoent&eos.app=x"));
        assertEquals("", OriginBackend.forwardable(""));
    }

    @Test
    void parsesRedirects() {
        RedirectException r = RedirectException.parse(1095, "pool7.example.org?org.dcache.uuid=u1");
        assertEquals("pool7.example.org", r.host);
        assertEquals(1095, r.port);
        assertEquals("org.dcache.uuid=u1", r.cgi);
        RedirectException u = RedirectException.parse(-1, "root://[2001:db8::1]:1096//path?x=1");
        assertEquals("2001:db8::1", u.host);
        assertEquals(1096, u.port);
    }

    private static byte[] bytes(ByteBuffer b) {
        byte[] out = new byte[b.remaining()];
        b.duplicate().get(out);
        return out;
    }
}
