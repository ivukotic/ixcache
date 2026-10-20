package org.atlasml.ixcache.health;

import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tells the VPS service this cache is up: a JSON POST of site, instance ID,
 * address and cache size (total disk capacity in kilobytes of 1024 bytes, as
 * {@code df} reports it), sent at most once per interval and only when
 * {@link #ready()} is called, i.e. when a readiness check passes. A cache that
 * stops being ready stops sending, and VPS drops it.
 *
 * <p>Like the {@code curl -k} it replaces, the client does not verify the
 * service's certificate. Only this status is sent; nothing is received.
 */
public final class Heartbeat {

    private static final Logger log = LoggerFactory.getLogger(Heartbeat.class);

    /**
     * @param url      VPS endpoint ({@code IXCACHE_HEARTBEAT_URL})
     * @param site     site name ({@code IXCACHE_SITE})
     * @param instance this cache's ID ({@code IXCACHE_INSTANCE})
     * @param address  host:port clients use ({@code IXCACHE_ADDRESS})
     * @param interval least time between heartbeats ({@code IXCACHE_HEARTBEAT_INTERVAL}, seconds)
     */
    public record Config(URI url, String site, String instance, String address, Duration interval) {

        /** Empty unless {@code IXCACHE_HEARTBEAT_URL} is set; then the other settings are required. */
        public static Optional<Config> fromEnv(Map<String, String> env) {
            String url = env.get("IXCACHE_HEARTBEAT_URL");
            if (url == null || url.isBlank()) {
                return Optional.empty();
            }
            return Optional.of(new Config(URI.create(url.trim()), required(env, "IXCACHE_SITE"),
                    required(env, "IXCACHE_INSTANCE"), required(env, "IXCACHE_ADDRESS"),
                    Duration.ofSeconds(Long.parseLong(env.getOrDefault("IXCACHE_HEARTBEAT_INTERVAL", "60")))));
        }

        private static String required(Map<String, String> env, String name) {
            String v = env.get(name);
            if (v == null || v.isBlank()) {
                throw new IllegalArgumentException(name + " is required when IXCACHE_HEARTBEAT_URL is set");
            }
            return v.trim();
        }
    }

    private final Config cfg;
    private final LongSupplier sizeBytes;
    private final HttpClient http;
    private final AtomicBoolean sending = new AtomicBoolean();
    private volatile Instant lastAttempt = Instant.EPOCH;
    private volatile String lastResult = "none sent yet";

    /** @param sizeBytes total size of the cache disks, in bytes */
    public Heartbeat(Config cfg, LongSupplier sizeBytes) {
        this.cfg = cfg;
        this.sizeBytes = sizeBytes;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .sslContext(trustAll())
                .build();
    }

    /** Called when a readiness check passes: sends a heartbeat if one is due. Never blocks. */
    public void ready() {
        Instant now = Instant.now();
        if (now.isBefore(lastAttempt.plus(cfg.interval())) || !sending.compareAndSet(false, true)) {
            return;
        }
        lastAttempt = now;
        String body = json(Map.of(
                "site", cfg.site(),
                "id", cfg.instance(),
                "address", cfg.address(),
                "size", String.valueOf(sizeBytes.getAsLong() / 1024)));
        HttpRequest req = HttpRequest.newBuilder(cfg.url())
                .timeout(Duration.ofSeconds(7))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        http.sendAsync(req, HttpResponse.BodyHandlers.discarding()).whenComplete((resp, err) -> {
            sending.set(false);
            if (err != null) {
                lastResult = "failed at " + now + ": " + err;
                log.warn("heartbeat to {} failed: {}", cfg.url(), err.toString());
            } else if (resp.statusCode() / 100 != 2) {
                lastResult = "HTTP " + resp.statusCode() + " at " + now;
                log.warn("heartbeat to {} got HTTP {}", cfg.url(), resp.statusCode());
            } else {
                lastResult = "HTTP " + resp.statusCode() + " at " + now;
                log.debug("heartbeat sent: {}", body);
            }
        });
    }

    /** The outcome of the most recent heartbeat, for the readiness report. */
    public String lastResult() {
        return lastResult;
    }

    /** A JSON object of string values. */
    static String json(Map<String, String> fields) {
        StringBuilder sb = new StringBuilder("{");
        // Fixed order, as in the shell version.
        for (String k : new String[] {"site", "id", "address", "size"}) {
            if (fields.containsKey(k)) {
                if (sb.length() > 1) {
                    sb.append(',');
                }
                sb.append(quote(k)).append(':').append(quote(fields.get(k)));
            }
        }
        return sb.append('}').toString();
    }

    static String quote(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    /** Accepts any server certificate and host name, as {@code curl -k} does. */
    private static SSLContext trustAll() {
        TrustManager tm = new X509ExtendedTrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) {
            }

            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {
            }

            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        };
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, new TrustManager[] {tm}, null);
            return ctx;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
