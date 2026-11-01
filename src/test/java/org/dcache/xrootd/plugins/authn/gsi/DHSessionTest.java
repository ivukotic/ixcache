package org.dcache.xrootd.plugins.authn.gsi;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.security.Security;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The client side of the DH exchange, as run against an origin. The peer's DH
 * parameters arrive without a private value length, which current BouncyCastle
 * rejected once xrootd4j filled it in as bitLength(p).
 */
class DHSessionTest {

    @BeforeAll
    static void registerProvider() {
        if (Security.getProvider("BC") == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    @Test
    void clientCompletesKeyAgreementWithServerParameters() throws Exception {
        DHSession server = new DHSession(true, 0);
        DHSession client = new DHSession(false, 0);

        client.finaliseKeyAgreement(server.getEncodedDHMaterial());
        server.finaliseKeyAgreement(client.getEncodedDHMaterial());

        byte[] plain = "cert step payload".getBytes(US_ASCII);
        byte[] encrypted = client.encrypt("AES/CBC/PKCS5Padding", "AES", 16, plain);
        assertArrayEquals(plain, server.decrypt("AES/CBC/PKCS5Padding", "AES", 16, encrypted));
    }
}
