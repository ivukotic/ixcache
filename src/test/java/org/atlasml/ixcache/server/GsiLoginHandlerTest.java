package org.atlasml.ixcache.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.StringWriter;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Properties;

import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;

import eu.emi.security.authn.x509.X509CertChainValidator;
import eu.emi.security.authn.x509.helpers.trust.OpensslTruststoreHelper;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.plugins.authn.gsi.CertChainValidatorProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The chain check that xrootd4j's GSI handler skips. A stock client cannot reach
 * it with an untrusted chain (the CA hash exchange fails first), so it is
 * exercised directly.
 */
class GsiLoginHandlerTest {

    @TempDir
    Path caDir;

    @Test
    void acceptsChainFromTrustedCa() throws Exception {
        Ca trusted = new Ca("Trusted CA");
        trusted.install(caDir);
        Login l = login(validator());

        l.handler.login(l.ctx, subject(trusted.issue("Test User")));

        assertEquals("CN=Test User,O=ixcache,DC=test",
                l.channel.attr(GsiLoginHandler.CLIENT_DN).get());
    }

    @Test
    void rejectsChainFromUntrustedCaWithSameDn() throws Exception {
        new Ca("Trusted CA").install(caDir);
        Ca rogue = new Ca("Rogue CA");
        Login l = login(validator());

        XrootdException e = assertThrows(XrootdException.class,
                () -> l.handler.login(l.ctx, subject(rogue.issue("Test User"))));
        assertEquals(org.dcache.xrootd.protocol.XrootdProtocol.kXR_NotAuthorized, e.getError());
        assertNull(l.channel.attr(GsiLoginHandler.CLIENT_DN).get());
    }

    @Test
    void rejectsSubjectWithoutChain() throws Exception {
        new Ca("Trusted CA").install(caDir);
        Login l = login(validator());

        assertThrows(XrootdException.class, () -> l.handler.login(l.ctx, new Subject()));
    }

    private record Login(GsiLoginHandler handler, EmbeddedChannel channel, ChannelHandlerContext ctx) {
    }

    private static Login login(X509CertChainValidator validator) {
        GsiLoginHandler h = new GsiLoginHandler(client -> null, validator);
        EmbeddedChannel ch = new EmbeddedChannel(h);
        return new Login(h, ch, ch.pipeline().context(h));
    }

    private X509CertChainValidator validator() throws Exception {
        Properties p = new Properties();
        p.setProperty("xrootd.gsi.ca.path", caDir.toString());
        p.setProperty("xrootd.gsi.ca.refresh", "1");
        p.setProperty("xrootd.gsi.ca.refresh.unit", "HOURS");
        p.setProperty("xrootd.gsi.ca.namespace-mode", "EUGRIDPMA_GLOBUS");
        p.setProperty("xrootd.gsi.ca.crl-mode", "IF_VALID");
        p.setProperty("xrootd.gsi.ca.ocsp-mode", "IGNORE");
        return new CertChainValidatorProvider(p).getCertChainValidator();
    }

    private static Subject subject(X509Certificate[] chain) {
        Subject s = new Subject();
        s.getPublicCredentials().add(chain);
        return s;
    }

    /** A throwaway certificate authority. */
    private static final class Ca {

        final KeyPair keys;
        final X509Certificate cert;

        Ca(String cn) throws Exception {
            keys = rsa();
            X500Principal name = new X500Principal("CN=" + cn + ",O=ixcache,DC=test");
            cert = sign(name, name, keys, keys, true);
        }

        X509Certificate[] issue(String cn) throws Exception {
            KeyPair user = rsa();
            X500Principal name = new X500Principal("CN=" + cn + ",O=ixcache,DC=test");
            return new X509Certificate[] {sign(name, cert.getSubjectX500Principal(), user, keys, false), cert};
        }

        void install(Path dir) throws Exception {
            StringWriter pem = new StringWriter();
            try (JcaPEMWriter w = new JcaPEMWriter(pem)) {
                w.writeObject(cert);
            }
            // Both hash names, as in the IGTF bundle; xrootd4j's validator uses the old one.
            for (boolean openssl1 : new boolean[] {false, true}) {
                String hash = OpensslTruststoreHelper.getOpenSSLCAHash(cert.getSubjectX500Principal(), openssl1);
                Files.writeString(dir.resolve(hash + ".0"), pem.toString());
            }
        }

        private static KeyPair rsa() throws Exception {
            KeyPairGenerator g = KeyPairGenerator.getInstance("RSA");
            g.initialize(2048);
            return g.generateKeyPair();
        }

        private static X509Certificate sign(X500Principal subject, X500Principal issuer,
                KeyPair subjectKeys, KeyPair issuerKeys, boolean ca) throws Exception {
            Instant now = Instant.now();
            JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(issuer,
                    BigInteger.valueOf(now.toEpochMilli()), Date.from(now.minus(1, ChronoUnit.HOURS)),
                    Date.from(now.plus(1, ChronoUnit.DAYS)), subject, subjectKeys.getPublic());
            b.addExtension(Extension.basicConstraints, true, new BasicConstraints(ca));
            b.addExtension(Extension.keyUsage, true, new KeyUsage(ca
                    ? KeyUsage.keyCertSign | KeyUsage.cRLSign
                    : KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
            return new JcaX509CertificateConverter().getCertificate(
                    b.build(new JcaContentSignerBuilder("SHA256withRSA").build(issuerKeys.getPrivate())));
        }
    }
}
