package org.atlasml.ixcache.health;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.atlasml.ixcache.health.Health.Check;
import org.atlasml.ixcache.util.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP endpoints for Kubernetes probes: {@code /healthz} (liveness) and
 * {@code /readyz} (readiness). Both answer 200 or 503 with a JSON report of the
 * individual checks. A passing readiness check also lets the VPS heartbeat go
 * out, if one is configured and due.
 */
public final class HealthServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(HealthServer.class);

    private final HttpServer http;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    private HealthServer(HttpServer http) {
        this.http = http;
    }

    /** Listens on {@code port} ({@code IXCACHE_HEALTH_PORT}). */
    public static HealthServer start(int port, Health health, Optional<Heartbeat> heartbeat) throws IOException {
        HttpServer http = HttpServer.create(new InetSocketAddress(port), 16);
        HealthServer s = new HealthServer(http);
        http.setExecutor(s.executor);
        http.createContext("/healthz", ex -> s.answer(ex, health::liveness, null));
        http.createContext("/readyz", ex -> s.answer(ex, health::readiness, heartbeat.orElse(null)));
        http.start();
        log.info("health endpoints on port {}: /healthz, /readyz{}", http.getAddress().getPort(),
                heartbeat.map(h -> "; VPS heartbeat enabled").orElse(""));
        return s;
    }

    public int port() {
        return http.getAddress().getPort();
    }

    private void answer(HttpExchange ex, Supplier<List<Check>> checks, Heartbeat heartbeat) throws IOException {
        try (ex) {
            if (!ex.getRequestMethod().equals("GET") && !ex.getRequestMethod().equals("HEAD")) {
                ex.sendResponseHeaders(405, -1);
                return;
            }
            List<Check> results;
            try {
                results = checks.get();
            } catch (RuntimeException e) {
                log.error("health check threw", e);
                results = List.of(new Check("internal", false, e.toString()));
            }
            boolean ok = Health.allOk(results);
            if (ok && heartbeat != null) {
                heartbeat.ready();
            }
            byte[] body = report(ok, results, heartbeat).getBytes(UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json");
            if (!ok) {
                log.warn("{} failing: {}", ex.getRequestURI().getPath(),
                        results.stream().filter(c -> !c.ok()).map(c -> c.name() + ": " + c.detail()).toList());
            }
            if (ex.getRequestMethod().equals("HEAD")) {
                ex.sendResponseHeaders(ok ? 200 : 503, -1);
                return;
            }
            ex.sendResponseHeaders(ok ? 200 : 503, body.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(body);
            }
        }
    }

    private static String report(boolean ok, List<Check> results, Heartbeat heartbeat) {
        StringBuilder sb = new StringBuilder("{\"status\":").append(Json.quote(ok ? "ok" : "fail"))
                .append(",\"checks\":{");
        for (int i = 0; i < results.size(); i++) {
            Check c = results.get(i);
            if (i > 0) {
                sb.append(',');
            }
            sb.append(Json.quote(c.name())).append(":{\"ok\":").append(c.ok())
                    .append(",\"detail\":").append(Json.quote(c.detail())).append('}');
        }
        sb.append('}');
        if (heartbeat != null) {
            sb.append(",\"heartbeat\":").append(Json.quote(heartbeat.lastResult()));
        }
        return sb.append("}\n").toString();
    }

    @Override
    public void close() {
        http.stop(0);
        executor.close();
    }
}
