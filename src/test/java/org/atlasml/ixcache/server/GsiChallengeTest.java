package org.atlasml.ixcache.server;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void rejectsTagsAClientCouldUseAsASigningOracle() {
        assertTrue(GSIRequestHandler.isWellFormedChallenge("aZ09./xY"));
        assertFalse(GSIRequestHandler.isWellFormedChallenge(null));
        assertFalse(GSIRequestHandler.isWellFormedChallenge("short"));
        assertFalse(GSIRequestHandler.isWellFormedChallenge("aZ09./xY0"));
        assertFalse(GSIRequestHandler.isWellFormedChallenge("aZ09./x\u0001"));
        // A PKCS#1 DigestInfo prefix is binary and at least 34 bytes long.
        assertFalse(GSIRequestHandler.isWellFormedChallenge("0010\r\n\u0006\t`\u0086H\u0001e\u0003\u0004\u0002\u0001"));
    }
}
