package org.atlasml.ixcache.telemetry;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.atlasml.ixcache.util.Json;

/**
 * What happened during one client's open of one file (schema v1).
 *
 * @param clientHost    client IP address
 * @param clientDn      authenticated DN, or "" without GSI
 * @param origin        origin host:port from the client path, or "" if none
 * @param path          path at the origin
 * @param cacheKey      cache key (Rucio scope:name or origin path), or ""
 * @param fileSize      file size in bytes
 * @param bytesOut      bytes sent to the client
 * @param bytesHit      bytes served from the cache disks
 * @param bytesFetched  bytes requested from the origin (whole blocks when caching)
 * @param readOps       kXR_read requests
 * @param readvOps      kXR_readv requests
 * @param readvElements elements in all kXR_readv requests
 * @param errors        requests answered with an error
 * @param durationMs    time from open to close
 * @param status        {@code closed} (client closed the file) or
 *                      {@code disconnected} (connection ended with the file open)
 */
public record TransferRecord(
        String clientHost,
        String clientDn,
        String origin,
        String path,
        String cacheKey,
        long fileSize,
        long bytesOut,
        long bytesHit,
        long bytesFetched,
        long readOps,
        long readvOps,
        long readvElements,
        long errors,
        long durationMs,
        String status) {

    public static final int SCHEMA_VERSION = 1;

    /**
     * The record as one JSON object of at most {@code maxBytes} UTF-8 bytes. Long
     * string fields are shortened (path, cache key, DN, origin, in that order)
     * until it fits; numbers are never dropped.
     */
    public byte[] toJson(Instant ts, String site, String instance, int maxBytes) {
        String p = path;
        String k = cacheKey;
        String dn = clientDn;
        String o = origin;
        for (int limit = 4096; ; limit /= 2) {
            byte[] b = render(ts, site, instance, p, k, dn, o).getBytes(UTF_8);
            if (b.length <= maxBytes || limit < 16) {
                return b;
            }
            p = cut(p, limit);
            k = cut(k, limit / 2);
            dn = cut(dn, limit / 2);
            o = cut(o, limit / 4);
        }
    }

    private String render(Instant ts, String site, String instance, String p, String k, String dn, String o) {
        return "{\"v\":" + SCHEMA_VERSION
                + ",\"ts\":" + Json.quote(ts.truncatedTo(ChronoUnit.MILLIS).toString())
                + ",\"site\":" + Json.quote(site)
                + ",\"instance\":" + Json.quote(instance)
                + ",\"client_host\":" + Json.quote(clientHost)
                + ",\"client_dn\":" + Json.quote(dn)
                + ",\"origin\":" + Json.quote(o)
                + ",\"path\":" + Json.quote(p)
                + ",\"cache_key\":" + Json.quote(k)
                + ",\"file_size\":" + fileSize
                + ",\"bytes_out\":" + bytesOut
                + ",\"bytes_hit\":" + bytesHit
                + ",\"bytes_fetched\":" + bytesFetched
                + ",\"read_ops\":" + readOps
                + ",\"readv_ops\":" + readvOps
                + ",\"readv_elements\":" + readvElements
                + ",\"errors\":" + errors
                + ",\"duration_ms\":" + durationMs
                + ",\"status\":" + Json.quote(status)
                + "}";
    }

    /** Keeps the start of {@code s}, marking the cut with "...". */
    private static String cut(String s, int max) {
        return s.length() <= max ? s : s.substring(0, Math.max(0, max - 3)) + "...";
    }
}
