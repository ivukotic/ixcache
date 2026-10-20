package org.atlasml.ixcache.server;

import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Front-end settings, read from the environment.
 *
 * @param port        TCP port clients connect to ({@code IXCACHE_PORT})
 * @param auth        client authentication ({@code IXCACHE_AUTH}: {@code none} or {@code gsi})
 * @param hostCert    PEM host certificate presented to GSI clients ({@code IXCACHE_HOSTCERT})
 * @param hostKey     PEM key for {@code hostCert} ({@code IXCACHE_HOSTKEY})
 * @param caDir       trusted CA certificates and CRLs ({@code X509_CERT_DIR})
 * @param localRoot   if set, serve files from this directory instead of origins
 *                    ({@code IXCACHE_LOCAL_ROOT}); for testing only
 * @param site        site name answered to {@code query config sitename}
 *                    ({@code IXCACHE_SITE}); the heartbeat container checks it
 */
public record ServerConfig(int port, Auth auth, Path hostCert, Path hostKey, Path caDir,
        Optional<Path> localRoot, Optional<String> site) {

    public static final int DEFAULT_PORT = 1092;

    /** Without a site name. */
    public ServerConfig(int port, Auth auth, Path hostCert, Path hostKey, Path caDir, Optional<Path> localRoot) {
        this(port, auth, hostCert, hostKey, caDir, localRoot, Optional.empty());
    }

    /** How clients authenticate. */
    public enum Auth {
        /** No client authentication, as in native XCache. */
        NONE,
        /** Mandatory GSI; needs a host certificate matching the name clients use. */
        GSI
    }

    public static ServerConfig fromEnv(Map<String, String> env) {
        return new ServerConfig(
                Integer.parseInt(env.getOrDefault("IXCACHE_PORT", String.valueOf(DEFAULT_PORT))),
                parseAuth(env.getOrDefault("IXCACHE_AUTH", "none")),
                Path.of(env.getOrDefault("IXCACHE_HOSTCERT", "/etc/grid-security/hostcert.pem")),
                Path.of(env.getOrDefault("IXCACHE_HOSTKEY", "/etc/grid-security/hostkey.pem")),
                Path.of(env.getOrDefault("X509_CERT_DIR", "/etc/grid-security/certificates")),
                Optional.ofNullable(env.get("IXCACHE_LOCAL_ROOT")).filter(s -> !s.isBlank()).map(Path::of),
                Optional.ofNullable(env.get("IXCACHE_SITE")).map(String::trim).filter(s -> !s.isEmpty()));
    }

    private static Auth parseAuth(String s) {
        try {
            return Auth.valueOf(s.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("IXCACHE_AUTH must be none or gsi, not '" + s + "'");
        }
    }
}
