package org.atlasml.ixcache.telemetry;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasml.ixcache.cache.Range;
import org.atlasml.ixcache.origin.OriginBackend;
import org.atlasml.ixcache.origin.OriginConfig;
import org.atlasml.ixcache.server.LocalBackend;
import org.atlasml.ixcache.server.OpenFile;
import org.atlasml.ixcache.server.ServerConfig;
import org.atlasml.ixcache.server.XrootdServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TelemetryTest {

    @TempDir
    Path dir;

    static TransferRecord sample(String path) {
        return new TransferRecord("192.170.240.106", "", "xrootd.aglt2.org:1094", path,
                "rucio:user.ivukotic:xcache.test.dat", 1048576, 1048576, 0, 1048576,
                1, 0, 0, 0, 71, "closed");
    }

    @Test
    void recordLayout() {
        String json = new String(sample("/atlaslocalgroupdisk/x.dat")
                .toJson(Instant.parse("2026-10-20T12:34:56.789123Z"), "MWT2", "xcache-uc-1", 1200), UTF_8);
        assertEquals("{\"v\":1,\"ts\":\"2026-10-20T12:34:56.789Z\",\"site\":\"MWT2\",\"instance\":\"xcache-uc-1\","
                + "\"client_host\":\"192.170.240.106\",\"client_dn\":\"\",\"origin\":\"xrootd.aglt2.org:1094\","
                + "\"path\":\"/atlaslocalgroupdisk/x.dat\",\"cache_key\":\"rucio:user.ivukotic:xcache.test.dat\","
                + "\"file_size\":1048576,\"bytes_out\":1048576,\"bytes_hit\":0,\"bytes_fetched\":1048576,"
                + "\"read_ops\":1,\"readv_ops\":0,\"readv_elements\":0,\"errors\":0,\"duration_ms\":71,"
                + "\"status\":\"closed\"}", json);
    }

    @Test
    void longFieldsAreShortenedToFitOneDatagram() {
        String longPath = "/" + "d/".repeat(2000) + "file.root";
        byte[] b = sample(longPath).toJson(Instant.now(), "MWT2", "xcache-uc-1", 1200);
        String json = new String(b, UTF_8);
        assertTrue(b.length <= 1200, "length " + b.length);
        assertTrue(json.contains("...\",\"cache_key\""), json);
        assertTrue(json.endsWith(",\"duration_ms\":71,\"status\":\"closed\"}"), json);
    }

    @Test
    void settings() {
        assertTrue(Telemetry.fromEnv(Map.of()).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> Telemetry.fromEnv(Map.of("IXCACHE_TELEMETRY", "nohost")));
    }

    @Test
    void transfersAreReportedOverUdp() throws Exception {
        byte[] content = new byte[300_000];
        new java.util.Random(4).nextBytes(content);
        Files.createDirectories(dir.resolve("export/data"));
        Files.write(dir.resolve("export/data/f.bin"), content);
        Files.createDirectories(dir.resolve("ca"));

        try (DatagramSocket collector = new DatagramSocket(0, InetAddress.getLoopbackAddress());
             ExecutorService io = Executors.newVirtualThreadPerTaskExecutor()) {
            collector.setSoTimeout(10_000);
            Telemetry telemetry = new Telemetry("127.0.0.1", collector.getLocalPort(), "MWT2", "xcache-uc-1");
            XrootdServer ixcache = XrootdServer.start(new ServerConfig(0, ServerConfig.Auth.NONE, null, null,
                    dir.resolve("ca"), Optional.empty()), new LocalBackend(dir.resolve("export"), io), telemetry);
            String path = "/root://127.0.0.1:" + ixcache.localAddress().getPort()
                    + "//root://origin.example.org:1095//data/f.bin";
            try {
                OriginBackend client = client();
                try (OpenFile f = client.open(path, "").get(30, TimeUnit.SECONDS)) {
                    f.readv(List.of(new Range(0, 1000))).get(30, TimeUnit.SECONDS);
                    f.readv(List.of(new Range(10, 10), new Range(5000, 200), new Range(299_000, 1000)))
                            .get(30, TimeUnit.SECONDS);
                }
                String r = receive(collector);
                assertEquals("closed", str(r, "status"));
                assertEquals("MWT2", str(r, "site"));
                assertEquals("xcache-uc-1", str(r, "instance"));
                assertEquals("127.0.0.1", str(r, "client_host"));
                assertEquals("origin.example.org:1095", str(r, "origin"));
                assertEquals("/data/f.bin", str(r, "path"));
                assertEquals("path:origin.example.org:1095/data/f.bin", str(r, "cache_key"));
                assertEquals(300_000, num(r, "file_size"));
                assertEquals(1, num(r, "read_ops"));
                assertEquals(1, num(r, "readv_ops"));
                assertEquals(3, num(r, "readv_elements"));
                assertEquals(2210, num(r, "bytes_out"));
                assertEquals(2210, num(r, "bytes_hit"));
                assertEquals(0, num(r, "errors"));

                // A connection that drops with the file still open.
                OriginBackend client2 = client();
                OpenFile f2 = client2.open(path, "").get(30, TimeUnit.SECONDS);
                f2.readv(List.of(new Range(0, 10))).get(30, TimeUnit.SECONDS);
                client2.close();
                String r2 = receive(collector);
                assertEquals("disconnected", str(r2, "status"));
                assertEquals(10, num(r2, "bytes_out"));
                client.close();
            } finally {
                ixcache.close();
                telemetry.close();
            }
            assertEquals(2, telemetry.sent());
            assertEquals(0, telemetry.dropped());
        }
    }

    private OriginBackend client() throws Exception {
        return new OriginBackend(new OriginConfig(Optional.empty(), dir.resolve("ca"), Duration.ofSeconds(3),
                Duration.ofSeconds(10), 1, 8, 2, 2, Duration.ofMinutes(1)));
    }

    private static String receive(DatagramSocket s) throws Exception {
        DatagramPacket p = new DatagramPacket(new byte[2048], 2048);
        s.receive(p);
        return new String(p.getData(), 0, p.getLength(), UTF_8);
    }

    private static String str(String json, String field) {
        Matcher m = Pattern.compile("\"" + field + "\":\"([^\"]*)\"").matcher(json);
        assertTrue(m.find(), field + " in " + json);
        return m.group(1);
    }

    private static long num(String json, String field) {
        Matcher m = Pattern.compile("\"" + field + "\":(\\d+)").matcher(json);
        assertNotNull(json);
        assertTrue(m.find(), field + " in " + json);
        return Long.parseLong(m.group(1));
    }
}
