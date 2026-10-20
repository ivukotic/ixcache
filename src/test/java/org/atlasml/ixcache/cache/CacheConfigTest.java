package org.atlasml.ixcache.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
        List<CacheConfig> cs = CacheConfig.fromEnv(Map.of(
                "IXCACHE_CACHE_DIR", dir.resolve("c").toString(),
                "IXCACHE_CACHE_SIZE", "10G",
                "IXCACHE_HIGH_WM", "0.8",
                "IXCACHE_LOW_WM", "0.7",
                "IXCACHE_BLOCK_SIZE", "256k"));
        assertEquals(1, cs.size());
        CacheConfig c = cs.get(0);
        assertEquals(10L << 30, c.capacityBytes());
        assertEquals(0.8, c.highWatermark());
        assertEquals(0.7, c.lowWatermark());
        assertEquals(256 * 1024, c.blockSize());
    }

    @Test
    void defaultsToTheFileSystemSize() throws Exception {
        CacheConfig c = CacheConfig.fromEnv(Map.of("IXCACHE_CACHE_DIR", dir.toString())).get(0);
        assertEquals(Files.getFileStore(dir).getTotalSpace(), c.capacityBytes());
        assertEquals(CacheConfig.DEFAULT_BLOCK_SIZE, c.blockSize());
    }

    @Test
    void severalDirectories() throws Exception {
        String dirs = dir.resolve("d1") + ", " + dir.resolve("d2") + ",,";
        List<CacheConfig> cs = CacheConfig.fromEnv(Map.of("IXCACHE_CACHE_DIR", dirs, "IXCACHE_CACHE_SIZE", "1G"));
        assertEquals(List.of(dir.resolve("d1"), dir.resolve("d2")), cs.stream().map(CacheConfig::root).toList());
        cs.forEach(c -> assertEquals(1L << 30, c.capacityBytes()));
    }

    @Test
    void directoriesSharingAFileSystemNeedAnExplicitSize() {
        String dirs = dir.resolve("d1") + "," + dir.resolve("d2");
        assertThrows(IllegalArgumentException.class, () -> CacheConfig.fromEnv(Map.of("IXCACHE_CACHE_DIR", dirs)));
    }

    @Test
    void unusableDirectoryIsLeftOut() throws Exception {
        Files.writeString(dir.resolve("afile"), "x");
        String dirs = dir.resolve("afile").resolve("sub") + "," + dir.resolve("ok");
        List<CacheConfig> cs = CacheConfig.fromEnv(Map.of("IXCACHE_CACHE_DIR", dirs, "IXCACHE_CACHE_SIZE", "1G"));
        assertEquals(List.of(dir.resolve("ok")), cs.stream().map(CacheConfig::root).toList());
    }
}
