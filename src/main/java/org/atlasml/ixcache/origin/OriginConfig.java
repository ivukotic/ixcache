package org.atlasml.ixcache.origin;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * Origin client settings.
 *
 * @param proxy              service proxy for GSI to origins ({@code X509_USER_PROXY});
 *                           without it only origins that need no authentication work
 * @param caDir              trusted CA certificates ({@code X509_CERT_DIR})
 * @param connectTimeout     TCP connect timeout
 * @param requestTimeout     how long one request may wait for its reply
 * @param maxConnsPerHost    connections kept per origin host
 * @param maxStreamsPerConn  outstanding requests on a connection before another is opened
 * @param maxRedirects       redirects followed for one open or stat
 * @param maxWaits           kXR_wait replies honoured for one request
 * @param idleTimeout        unused connections are closed after this long
 */
public record OriginConfig(
        Optional<Path> proxy,
        Path caDir,
        Duration connectTimeout,
        Duration requestTimeout,
        int maxConnsPerHost,
        int maxStreamsPerConn,
        int maxRedirects,
        int maxWaits,
        Duration idleTimeout) {

    public static OriginConfig fromEnv(Map<String, String> env) {
        return new OriginConfig(
                Optional.ofNullable(env.get("X509_USER_PROXY")).filter(s -> !s.isBlank()).map(Path::of),
                Path.of(env.getOrDefault("X509_CERT_DIR", "/etc/grid-security/certificates")),
                Duration.ofSeconds(10),
                Duration.ofSeconds(Long.parseLong(env.getOrDefault("IXCACHE_ORIGIN_TIMEOUT", "120"))),
                4,
                64,
                16,
                10,
                Duration.ofMinutes(5));
    }
}
