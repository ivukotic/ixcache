package org.atlasml.ixcache.path;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;

import org.atlasml.ixcache.cache.CacheKey;

/**
 * A client open path of the form {@code /root://origin[:port]//path[?opaque]}.
 *
 * <p>The origin is taken from the embedded URL. The opaque (CGI) part is kept so
 * it can be forwarded to the origin, but it never takes part in the cache key.
 */
public record RequestPath(String scheme, String host, int port, String path, String opaque) {

    public static final int DEFAULT_PORT = 1094;

    public RequestPath {
        if (!scheme.equals("root") && !scheme.equals("roots")) {
            throw new IllegalArgumentException("unsupported scheme: " + scheme);
        }
        if (host.isEmpty()) {
            throw new IllegalArgumentException("missing origin host");
        }
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("origin path must be absolute: " + path);
        }
        opaque = opaque == null ? "" : opaque;
    }

    /** Parses the path a client sent in {@code kXR_open}. */
    public static RequestPath parse(String clientPath) {
        String s = clientPath;
        while (s.startsWith("/")) {
            s = s.substring(1);
        }
        int schemeEnd = s.indexOf("://");
        if (schemeEnd < 0) {
            throw new IllegalArgumentException("path does not embed an origin URL: " + clientPath);
        }
        String scheme = s.substring(0, schemeEnd);
        String rest = s.substring(schemeEnd + 3);

        int slash = rest.indexOf('/');
        if (slash < 0) {
            throw new IllegalArgumentException("origin URL has no path: " + clientPath);
        }
        String authority = rest.substring(0, slash);
        String pathAndQuery = rest.substring(slash + 1); // drop the separator, keep "//x" -> "/x"

        String opaque = "";
        int q = pathAndQuery.indexOf('?');
        if (q >= 0) {
            opaque = pathAndQuery.substring(q + 1);
            pathAndQuery = pathAndQuery.substring(0, q);
        }
        String path = pathAndQuery.startsWith("/") ? pathAndQuery : "/" + pathAndQuery;

        String host;
        int port = DEFAULT_PORT;
        try {
            URI uri = new URI(null, authority, null, null, null);
            host = uri.getHost();
            if (uri.getPort() >= 0) {
                port = uri.getPort();
            }
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("bad origin authority: " + authority, e);
        }
        if (host == null) {
            throw new IllegalArgumentException("bad origin authority: " + authority);
        }
        return new RequestPath(scheme, host, port, path, opaque);
    }

    /** Origin URL without opaque data, e.g. {@code root://host:1094//path}. */
    public String originUrl() {
        return scheme + "://" + host + ":" + port + "/" + path;
    }

    /**
     * Cache key: Rucio {@code scope:name} when the path follows the deterministic
     * Rucio layout, otherwise the origin host, port and path.
     */
    public CacheKey cacheKey() {
        return rucioDid(path)
                .map(did -> new CacheKey("rucio:" + did))
                .orElseGet(() -> new CacheKey("path:" + host + ":" + port + path));
    }

    /**
     * Returns {@code scope:name} when {@code path} contains
     * {@code /rucio/<scope parts>/<xx>/<yy>/<name>} and {@code xx/yy} are the first
     * two bytes of {@code md5("scope:name")}.
     */
    static Optional<String> rucioDid(String path) {
        String[] parts = path.split("/");
        for (int i = 0; i < parts.length; i++) {
            if (!parts[i].equals("rucio")) {
                continue;
            }
            // need at least one scope part, xx, yy and name after "rucio"
            int n = parts.length;
            if (n - i - 1 < 4) {
                continue;
            }
            String name = parts[n - 1];
            String yy = parts[n - 2];
            String xx = parts[n - 3];
            String scope = String.join(".", java.util.Arrays.copyOfRange(parts, i + 1, n - 3));
            if (scope.isEmpty() || name.isEmpty()) {
                continue;
            }
            String did = scope + ":" + name;
            String hex = md5Hex(did);
            if (hex.substring(0, 2).equals(xx) && hex.substring(2, 4).equals(yy)) {
                return Optional.of(did);
            }
        }
        return Optional.empty();
    }

    private static String md5Hex(String s) {
        try {
            byte[] d = MessageDigest.getInstance("MD5").digest(s.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(d);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
