package org.atlasml.ixcache.health;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.atlasml.ixcache.cache.Cache;
import org.atlasml.ixcache.cache.MultiCache;
import org.atlasml.ixcache.origin.OriginBackend;
import org.atlasml.ixcache.server.XrootdServer;

/**
 * Liveness and readiness checks.
 *
 * <p>Live: the XRootD port is open and the network threads respond, so a
 * restart would not help otherwise. Ready: live, plus at least one cache disk
 * is writable, the service proxy exists and has not expired, and the CA
 * directory holds certificates; a cache failing these cannot serve misses.
 */
public final class Health {

    /** One check's outcome. */
    public record Check(String name, boolean ok, String detail) {
    }

    private static final Duration LOOP_TIMEOUT = Duration.ofSeconds(2);

    private final XrootdServer server;
    private final MultiCache cache;
    private final OriginBackend origin;
    private final Path caDir;

    /**
     * @param cache  may be null (no cache configured)
     * @param origin may be null (serving local files)
     * @param caDir  checked only when {@code origin} has a proxy
     */
    public Health(XrootdServer server, MultiCache cache, OriginBackend origin, Path caDir) {
        this.server = server;
        this.cache = cache;
        this.origin = origin;
        this.caDir = caDir;
    }

    public List<Check> liveness() {
        List<Check> out = new ArrayList<>();
        out.add(new Check("xrootd", server.isListening(),
                server.isListening() ? "listening on " + server.localAddress().getPort() : "not listening"));
        boolean responsive = server.eventLoopsResponsive(LOOP_TIMEOUT);
        out.add(new Check("event-loops", responsive,
                responsive ? "responsive" : "a network thread did not respond within " + LOOP_TIMEOUT.toSeconds() + "s"));
        return out;
    }

    public List<Check> readiness() {
        List<Check> out = liveness();
        if (cache != null) {
            out.add(cacheCheck());
        }
        if (origin != null) {
            Optional<Instant> expiry;
            try {
                expiry = origin.proxyExpiry();
            } catch (IOException e) {
                out.add(new Check("proxy", false, e.getMessage()));
                return out;
            }
            if (expiry.isPresent()) {
                Instant now = Instant.now();
                boolean valid = expiry.get().isAfter(now);
                out.add(new Check("proxy", valid,
                        (valid ? "expires " : "expired ") + expiry.get()));
                out.add(caCheck());
            }
        }
        return out;
    }

    private Check cacheCheck() {
        List<String> bad = new ArrayList<>();
        for (Cache c : cache.disks()) {
            Path root = c.config().root();
            if (!Files.isDirectory(root) || !Files.isWritable(root)) {
                bad.add(root.toString());
            }
        }
        int good = cache.disks().size() - bad.size();
        String detail = good + "/" + cache.disks().size() + " disks writable, "
                + cache.usedBytes() + " of " + cache.capacityBytes() + " bytes used"
                + (bad.isEmpty() ? "" : "; not writable: " + bad);
        return new Check("cache", good > 0, detail);
    }

    private Check caCheck() {
        try (Stream<Path> s = Files.list(caDir)) {
            long n = s.filter(p -> p.getFileName().toString().endsWith(".0")).count();
            return new Check("ca-certificates", n > 0, n + " CA certificates in " + caDir);
        } catch (IOException e) {
            return new Check("ca-certificates", false, "cannot read " + caDir + ": " + e.getMessage());
        }
    }

    static boolean allOk(List<Check> checks) {
        return checks.stream().allMatch(Check::ok);
    }
}
