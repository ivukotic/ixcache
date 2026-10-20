package org.atlasml.ixcache.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CacheConfigTest {

    @TempDir
    Path dir;

    @Test
    void parsesSizes() {
        assertEquals(1048576, CacheConfig.parseBytes("1048576"));
        assertEquals(64 * 1024, CacheConfig.parseBytes("64k"));
        assertEquals(256 * 1024, CacheConfig.parseBytes("256KB"));
        assertEquals(500L << 30, CacheConfig.parseBytes("500G"));
        assertEquals(2L << 40, CacheConfig.parseBytes(" 2T "));
        assertThrows(IllegalArgumentException.class, () -> CacheConfig.parseBytes("lots"));
        assertThrows(IllegalArgumentException.class, () -> CacheConfig.parseBytes("99999999T"));
    }

    @Test
    void readsEnvironment() throws Exception {
        CacheConfig c = CacheConfig.fromEnv(Map.of(
                "IXCACHE_CACHE_DIR", dir.resolve("c").toString(),
                "IXCACHE_CACHE_SIZE", "10G",
                "IXCACHE_HIGH_WM", "0.8",
                "IXCACHE_LOW_WM", "0.7",
                "IXCACHE_BLOCK_SIZE", "256k"));
        assertEquals(10L << 30, c.capacityBytes());
        assertEquals(0.8, c.highWatermark());
        assertEquals(0.7, c.lowWatermark());
        assertEquals(256 * 1024, c.blockSize());
    }

    @Test
    void defaultsToTheFileSystemSize() throws Exception {
        CacheConfig c = CacheConfig.fromEnv(Map.of("IXCACHE_CACHE_DIR", dir.toString()));
        assertEquals(java.nio.file.Files.getFileStore(dir).getTotalSpace(), c.capacityBytes());
        assertEquals(CacheConfig.DEFAULT_BLOCK_SIZE, c.blockSize());
    }
}
