package org.atlasml.ixcache.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringWriter;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.security.auth.x500.X500Principal;

import com.sun.net.httpserver.HttpServer;
import org.atlasml.ixcache.cache.CacheConfig;
import org.atlasml.ixcache.cache.MultiCache;
import org.atlasml.ixcache.origin.OriginBackend;
import org.atlasml.ixcache.origin.OriginConfig;
import org.atlasml.ixcache.server.LocalBackend;
import org.atlasml.ixcache.server.ServerConfig;
import org.atlasml.ixcache.server.XrootdServer;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HealthServerTest {

    @TempDir
    Path dir;

    ExecutorService io;
    XrootdServer xrootd;
    MultiCache cache;
    OriginBackend origin;
    HttpServer vps;
    List<String> beats = new CopyOnWriteArrayList<>();
    HealthServer health;
    HttpClient http = HttpClient.newHttpClient();
    Path proxy;

    @BeforeEach
    void start() throws Exception {
        io = Executors.newVirtualThreadPerTaskExecutor();
        Path ca = Files.createDirectories(dir.resolve("ca"));
        Files.writeString(ca.resolve("abcd1234.0"), "not checked here");
        proxy = dir.resolve("x509up");
        xrootd = XrootdServer.start(new ServerConfig(0, ServerConfig.Auth.NONE, null, null, ca, Optional.empty()),
                new LocalBackend(dir, io));
        cache = MultiCache.open(List.of(
                CacheConfig.defaults(dir.resolve("d1"), 3L << 30),
                CacheConfig.defaults(dir.resolve("d2"), 5L << 30)));
        origin = new OriginBackend(new OriginConfig(Optional.of(proxy), ca, Duration.ofSeconds(3),
                Duration.ofSeconds(10), 2, 8, 4, 2, Duration.ofMinutes(1)));

        vps = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        vps.createContext("/liveness", ex -> {
            beats.add(ex.getRequestHeaders().getFirst("Content-Type") + " "
                    + new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            ex.sendResponseHeaders(200, -1);
            ex.close();
        });
        vps.start();
    }

    void startHealth(Duration interval) throws Exception {
        Heartbeat.Config hc = new Heartbeat.Config(
                URI.create("http://127.0.0.1:" + vps.getAddress().getPort() + "/liveness"),
                "MWT2", "xcache-uc-1", "192.170.240.18:1092", interval);
        health = HealthServer.start(0, new Health(xrootd, cache, origin, dir.resolve("ca")),
                Optional.of(new Heartbeat(hc, cache::capacityBytes)));
    }

    @AfterEach
    void stop() throws Exception {
        if (health != null) {
            health.close();
        }
        vps.stop(0);
        origin.close();
        cache.close();
        xrootd.close();
        io.close();
    }

    @Test
    void readyCacheSendsHeartbeatsAtMostOncePerInterval() throws Exception {
        writeProxy(Instant.now().plus(Duration.ofHours(12)));
        startHealth(Duration.ofSeconds(1));

        HttpResponse<String> live = get("/healthz");
        assertEquals(200, live.statusCode(), live.body());

        HttpResponse<String> ready = get("/readyz");
        assertEquals(200, ready.statusCode(), ready.body());
        assertTrue(ready.body().contains("\"cache\":{\"ok\":true"), ready.body());
        awaitBeats(1);
        assertEquals("application/json {\"site\":\"MWT2\",\"id\":\"xcache-uc-1\","
                + "\"address\":\"192.170.240.18:1092\",\"size\":\"" + (8L << 20) + "\"}", beats.get(0));

        get("/readyz");
        get("/readyz");
        Thread.sleep(300);
        assertEquals(1, beats.size(), "heartbeats within one interval");

        Thread.sleep(1000);
        assertEquals(200, get("/readyz").statusCode());
        awaitBeats(2);
        assertTrue(get("/readyz").body().contains("\"heartbeat\":\"HTTP 200 at "));
    }

    @Test
    void expiredProxyIsNotReadyAndSendsNothing() throws Exception {
        writeProxy(Instant.now().minus(Duration.ofMinutes(5)));
        startHealth(Duration.ofSeconds(1));

        assertEquals(200, get("/healthz").statusCode());
        HttpResponse<String> ready = get("/readyz");
        assertEquals(503, ready.statusCode());
        assertTrue(ready.body().contains("\"proxy\":{\"ok\":false,\"detail\":\"expired "), ready.body());
        Thread.sleep(500);
        assertEquals(0, beats.size());
    }

    @Test
    void missingProxyIsNotReady() throws Exception {
        startHealth(Duration.ofSeconds(1));
        HttpResponse<String> ready = get("/readyz");
        assertEquals(503, ready.statusCode());
        assertTrue(ready.body().contains("does not exist"), ready.body());
        assertEquals(0, beats.size());
    }

    @Test
    void stoppedXrootdIsNotLive() throws Exception {
        writeProxy(Instant.now().plus(Duration.ofHours(1)));
        startHealth(Duration.ofSeconds(1));
        xrootd.close();
        HttpResponse<String> live = get("/healthz");
        assertEquals(503, live.statusCode());
        assertTrue(live.body().contains("not listening"), live.body());
    }

    @Test
    void heartbeatSettingsAreRequiredTogether() {
        assertTrue(Heartbeat.Config.fromEnv(java.util.Map.of()).isEmpty());
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> Heartbeat.Config.fromEnv(java.util.Map.of("IXCACHE_HEARTBEAT_URL", "https://vps.cern.ch/liveness")));
        Heartbeat.Config c = Heartbeat.Config.fromEnv(java.util.Map.of(
                "IXCACHE_HEARTBEAT_URL", "https://vps.cern.ch/liveness", "IXCACHE_SITE", "MWT2",
                "IXCACHE_INSTANCE", "xcache-uc-1", "IXCACHE_ADDRESS", "192.170.240.18:1092")).orElseThrow();
        assertEquals(Duration.ofSeconds(60), c.interval());
        assertEquals("{\"site\":\"a\\\"b\",\"id\":\"x\"}",
                Heartbeat.json(java.util.Map.of("site", "a\"b", "id", "x")));
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + health.port() + path)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private void awaitBeats(int n) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (beats.size() < n && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(n, beats.size());
    }

    /** A self-signed certificate and key in one PEM file, standing in for a proxy. */
    private void writeProxy(Instant notAfter) throws Exception {
        KeyPairGenerator g = KeyPairGenerator.getInstance("RSA");
        g.initialize(2048);
        KeyPair kp = g.generateKeyPair();
        X500Principal name = new X500Principal("CN=proxy,O=ixcache");
        var cert = new JcaX509CertificateConverter().getCertificate(new JcaX509v3CertificateBuilder(name,
                BigInteger.ONE, Date.from(notAfter.minus(Duration.ofDays(1))), Date.from(notAfter), name,
                kp.getPublic()).build(new JcaContentSignerBuilder("SHA256withRSA").build(kp.getPrivate())));
        StringWriter pem = new StringWriter();
        try (JcaPEMWriter w = new JcaPEMWriter(pem)) {
            w.writeObject(cert);
            w.writeObject(kp.getPrivate());
        }
        Files.writeString(proxy, pem.toString());
    }
}
