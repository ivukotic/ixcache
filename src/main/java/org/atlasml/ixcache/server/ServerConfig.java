package org.atlasml.ixcache.server;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

/**
 * Front-end settings, read from the environment.
 *
 * @param port        TCP port clients connect to ({@code IXCACHE_PORT})
 * @param hostCert    PEM host certificate presented to GSI clients ({@code IXCACHE_HOSTCERT})
 * @param hostKey     PEM key for {@code hostCert} ({@code IXCACHE_HOSTKEY})
 * @param caDir       trusted CA certificates and CRLs ({@code X509_CERT_DIR})
 * @param localRoot   if set, serve files from this directory instead of origins
 *                    ({@code IXCACHE_LOCAL_ROOT}); for testing only
 */
public record ServerConfig(int port, Path hostCert, Path hostKey, Path caDir,
        Optional<Path> localRoot) {

    public static final int DEFAULT_PORT = 1092;

    public static ServerConfig fromEnv(Map<String, String> env) {
        return new ServerConfig(
                Integer.parseInt(env.getOrDefault("IXCACHE_PORT", String.valueOf(DEFAULT_PORT))),
                Path.of(env.getOrDefault("IXCACHE_HOSTCERT", "/etc/grid-security/hostcert.pem")),
                Path.of(env.getOrDefault("IXCACHE_HOSTKEY", "/etc/grid-security/hostkey.pem")),
                Path.of(env.getOrDefault("X509_CERT_DIR", "/etc/grid-security/certificates")),
                Optional.ofNullable(env.get("IXCACHE_LOCAL_ROOT")).filter(s -> !s.isBlank()).map(Path::of));
    }
}
