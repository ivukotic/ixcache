package org.atlasml.ixcache.cache;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Identity of a cached file. Equal keys share cached bytes. */
public record CacheKey(String id) {

    public CacheKey {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("empty cache key");
        }
    }

    /** File-system-safe name derived from the key (SHA-256 hex). */
    public String fileName() {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(id.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(d);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
