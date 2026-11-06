package org.atlasml.ixcache.health;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509CRL;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.atlasml.ixcache.cache.Cache;
import org.atlasml.ixcache.cache.MultiCache;
import org.atlasml.ixcache.origin.OriginBackend;
import org.atlasml.ixcache.server.XrootdServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Liveness and readiness checks.
 *
 * <p>Live: the XRootD port is open and the network threads respond, so a
 * restart would not help otherwise. Ready: live, plus at least one cache disk
 * is writable, the service proxy exists and has not expired, and the CA
 * directory holds certificates; a cache failing these cannot serve misses.
 *
 * <p>Expired or unreadable CRLs in the CA directory are a warning, not a
 * failure: origins whose certificates come from those CAs fail GSI login, but
 * the others still work, and an unready cache would stop its VPS heartbeat.
 */
public final class Health {

    /** One check's outcome; a {@code warning} is reported but does not fail the check. */
    public record Check(String name, boolean ok, String detail, boolean warning) {

        public Check(String name, boolean ok, String detail) {
            this(name, ok, detail, false);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Health.class);

    private static final Duration LOOP_TIMEOUT = Duration.ofSeconds(2);
    /** CRL files as fetch-crl names them: the CA's subject hash, then .r0, .r1, ... */
    private static final Pattern CRL_FILE = Pattern.compile("[0-9a-f]{8}\\.r\\d+");

    private final XrootdServer server;
    private final MultiCache cache;
    private final OriginBackend origin;
    private final Path caDir;
    /** Parsed CRLs by file, re-read only when a file changes. Guarded by this. */
    private final Map<Path, Crl> crls = new HashMap<>();
    private String lastCrlWarning; // guarded by this

    /** A CRL file's issuer and next update, as of the file's size and modification time. */
    private record Crl(FileTime modified, long size, String issuer, Instant nextUpdate) {
    }

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
                out.add(crlCheck(now));
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

    /**
     * Warns about CRLs past their next update, which canl rejects, failing GSI
     * login to every origin with a certificate from that CA. A CA is listed once
     * although its CRL is installed under both of its subject hashes.
     */
    synchronized Check crlCheck(Instant now) {
        Set<String> issuers = new HashSet<>();
        Map<String, Instant> expired = new TreeMap<>();
        Set<String> unreadable = new TreeSet<>();
        Set<Path> seen = new HashSet<>();
        try (Stream<Path> s = Files.list(caDir)) {
            for (Path p : (Iterable<Path>) s.filter(p -> CRL_FILE.matcher(p.getFileName().toString()).matches())::iterator) {
                seen.add(p);
                Crl c = crl(p);
                if (c == null) {
                    unreadable.add(p.getFileName().toString());
                    continue;
                }
                issuers.add(c.issuer());
                if (c.nextUpdate() != null && c.nextUpdate().isBefore(now)) {
                    expired.put(c.issuer(), c.nextUpdate());
                }
            }
        } catch (IOException e) {
            return new Check("crls", true, "cannot read " + caDir + ": " + e.getMessage(), true);
        }
        crls.keySet().retainAll(seen);

        List<String> problems = new ArrayList<>();
        if (!expired.isEmpty()) {
            problems.add(expired.size() + " of " + issuers.size() + " CRLs expired: " + String.join("; ",
                    expired.entrySet().stream().map(e -> e.getKey() + " (next update " + e.getValue() + ")").toList()));
        }
        if (!unreadable.isEmpty()) {
            problems.add("unreadable: " + String.join(", ", unreadable));
        }
        String warning = problems.isEmpty() ? null : String.join("; ", problems);
        if (warning != null && !warning.equals(lastCrlWarning)) {
            log.warn("CRLs in {}: {}", caDir, warning);
        } else if (warning == null && lastCrlWarning != null) {
            log.info("all {} CRLs in {} are current again", issuers.size(), caDir);
        }
        lastCrlWarning = warning;
        return warning == null
                ? new Check("crls", true, issuers.size() + " CRLs current")
                : new Check("crls", true, warning, true);
    }

    /** The CRL in {@code p}, parsed again only if the file changed; null if unreadable. */
    private Crl crl(Path p) {
        try {
            FileTime modified = Files.getLastModifiedTime(p);
            long size = Files.size(p);
            Crl c = crls.get(p);
            if (c != null && c.modified().equals(modified) && c.size() == size) {
                return c;
            }
            X509CRL x;
            try (InputStream in = Files.newInputStream(p)) {
                x = (X509CRL) CertificateFactory.getInstance("X.509").generateCRL(in);
            }
            c = new Crl(modified, size, x.getIssuerX500Principal().getName(),
                    x.getNextUpdate() == null ? null : x.getNextUpdate().toInstant());
            crls.put(p, c);
            return c;
        } catch (IOException | GeneralSecurityException | RuntimeException e) {
            crls.remove(p);
            return null;
        }
    }

    static boolean allOk(List<Check> checks) {
        return checks.stream().allMatch(Check::ok);
    }
}
