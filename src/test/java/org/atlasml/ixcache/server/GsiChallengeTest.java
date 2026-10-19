package org.atlasml.ixcache.server;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import org.dcache.xrootd.plugins.authn.gsi.GSIRequestHandler;
import org.junit.jupiter.api.Test;

/** The patched xrootd4j random tag handling, which XRootD 6 clients depend on. */
class GsiChallengeTest {

    @Test
    void patchedClassIsLoaded() {
        assertDoesNotThrow(XrootdServer::checkPatchedGsi);
    }

    @Test
    void generatedTagsAreWellFormed() {
        for (int i = 0; i < 10_000; i++) {
            String tag = GSIRequestHandler.generateChallengeString();
            assertTrue(tag.matches("[A-Za-z0-9./]{8}"), tag);
        }
    }

    @Test
    void checksGeneratedTagFormat() {
        assertTrue(GSIRequestHandler.isWellFormedChallenge("aZ09./xY"));
        assertFalse(GSIRequestHandler.isWellFormedChallenge(null));
        assertFalse(GSIRequestHandler.isWellFormedChallenge("short"));
        assertFalse(GSIRequestHandler.isWellFormedChallenge("aZ09./xY0"));
        assertFalse(GSIRequestHandler.isWellFormedChallenge("aZ09./x\u0001"));
    }

    @Test
    void signsOnlyTagsThatCannotBeADigestInfo() {
        // Stock xrootd4j peers (dCache) send 8 arbitrary ASCII bytes: must be signed.
        assertTrue(GSIRequestHandler.isSafeToSign(new byte[] {1, 31, 64, 126, 0, 9, 10, 13}));
        assertTrue(GSIRequestHandler.isSafeToSign("aZ09./xY".getBytes(StandardCharsets.US_ASCII)));
        assertFalse(GSIRequestHandler.isSafeToSign(null));
        assertFalse(GSIRequestHandler.isSafeToSign(new byte[7]));
        // A SHA-256 DigestInfo is 51 bytes.
        assertFalse(GSIRequestHandler.isSafeToSign(new byte[51]));
    }
}
