package org.atlasml.ixcache.path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class RequestPathTest {

    static final String README_PATH =
            "/root://xrootd.aglt2.org:1094//atlaslocalgroupdisk/rucio/user/ivukotic/7d/9b/xcache.test.dat";

    @Test
    void parsesEmbeddedOriginUrl() {
        RequestPath p = RequestPath.parse(README_PATH);
        assertEquals("root", p.scheme());
        assertEquals("xrootd.aglt2.org", p.host());
        assertEquals(1094, p.port());
        assertEquals("/atlaslocalgroupdisk/rucio/user/ivukotic/7d/9b/xcache.test.dat", p.path());
        assertEquals("root://xrootd.aglt2.org:1094//atlaslocalgroupdisk/rucio/user/ivukotic/7d/9b/xcache.test.dat",
                p.originUrl());
    }

    @Test
    void acceptsCollapsedSlashesFromRoot() {
        // ROOT's TUrl turns "//" into "/" before sending the open.
        RequestPath p = RequestPath.parse(
                "/root:/xrootd.aglt2.org:1094/atlaslocalgroupdisk/rucio/user/ivukotic/7d/9b/xcache.test.dat");
        assertEquals(RequestPath.parse(README_PATH), p);
        assertEquals(RequestPath.parse(README_PATH).cacheKey(), p.cacheKey());
    }

    @Test
    void defaultPortAndOpaque() {
        RequestPath p = RequestPath.parse("/roots://host.example//a/b?xrd.wantprot=gsi&x=1");
        assertEquals("roots", p.scheme());
        assertEquals(1094, p.port());
        assertEquals("/a/b", p.path());
        assertEquals("xrd.wantprot=gsi&x=1", p.opaque());
    }

    @Test
    void rucioPathsShareKeyAcrossOrigins() {
        RequestPath a = RequestPath.parse(README_PATH);
        RequestPath b = RequestPath.parse(
                "/root://other.site:1095//pnfs/site/atlasscratchdisk/rucio/user/ivukotic/7d/9b/xcache.test.dat?x=y");
        assertEquals("rucio:user.ivukotic:xcache.test.dat", a.cacheKey().id());
        assertEquals(a.cacheKey(), b.cacheKey());
    }

    @Test
    void hashMismatchFallsBackToOriginPath() {
        RequestPath p = RequestPath.parse("/root://h:1094//disk/rucio/user/ivukotic/00/00/xcache.test.dat");
        assertEquals("path:h:1094/disk/rucio/user/ivukotic/00/00/xcache.test.dat", p.cacheKey().id());
    }

    @Test
    void nonRucioPathUsesOriginAndPath() {
        RequestPath p = RequestPath.parse("/root://h//store/data/file.root");
        assertEquals("path:h:1094/store/data/file.root", p.cacheKey().id());
    }

    @Test
    void rejectsPathsWithoutOrigin() {
        assertThrows(IllegalArgumentException.class, () -> RequestPath.parse("/store/data/file.root"));
        assertThrows(IllegalArgumentException.class, () -> RequestPath.parse("/http://h//f"));
        assertThrows(IllegalArgumentException.class, () -> RequestPath.parse("/root://h"));
    }
}
