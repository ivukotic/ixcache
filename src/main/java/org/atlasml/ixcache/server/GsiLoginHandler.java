package org.atlasml.ixcache.server;

import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_NotAuthorized;

import java.security.cert.X509Certificate;
import java.util.Set;

import javax.security.auth.Subject;

import eu.emi.security.authn.x509.ValidationResult;
import eu.emi.security.authn.x509.X509CertChainValidator;
import eu.emi.security.authn.x509.proxy.ProxyUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import org.dcache.xrootd.core.XrootdAuthenticationHandler;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.plugins.AuthenticationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the GSI exchange for one connection and accepts the client only if its
 * certificate chain validates against the trusted CAs.
 *
 * <p>xrootd4j's GSI handler proves the client holds the proxy's private key but
 * ignores the result of its own chain validation, so the check is done here.
 */
final class GsiLoginHandler extends XrootdAuthenticationHandler {

    private static final Logger log = LoggerFactory.getLogger(GsiLoginHandler.class);

    /** The authenticated client's end-entity DN, for logging and telemetry. */
    static final AttributeKey<String> CLIENT_DN = AttributeKey.valueOf("ixcache.clientDn");

    private final X509CertChainValidator validator;

    GsiLoginHandler(AuthenticationFactory gsi, X509CertChainValidator validator) {
        super("gsi", gsi, null);
        this.validator = validator;
    }

    @Override
    protected Subject login(ChannelHandlerContext ctx, Subject subject) throws XrootdException {
        Set<X509Certificate[]> chains = subject.getPublicCredentials(X509Certificate[].class);
        if (chains.isEmpty()) {
            throw new XrootdException(kXR_NotAuthorized, "no certificate chain presented");
        }
        X509Certificate[] chain = chains.iterator().next();
        ValidationResult r = validator.validate(chain);
        if (!r.isValid()) {
            log.info("{} rejected {}: {}", ctx.channel().remoteAddress(),
                    chain[0].getSubjectX500Principal().getName(), r.getErrors());
            throw new XrootdException(kXR_NotAuthorized, "certificate chain is not valid");
        }
        String dn = ProxyUtils.getOriginalUserDN(chain).getName();
        ctx.channel().attr(CLIENT_DN).set(dn);
        log.info("{} authenticated as {}", ctx.channel().remoteAddress(), dn);
        return subject;
    }
}
