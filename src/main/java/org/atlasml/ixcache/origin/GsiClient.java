package org.atlasml.ixcache.origin;

import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_NotAuthorized;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ServerError;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_authmore;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ok;
import static org.dcache.xrootd.security.XrootdSecurityProtocol.kXGS_cert;

import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import eu.emi.security.authn.x509.X509CertChainValidator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.atlasml.ixcache.gsi.GsiProperties;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.plugins.authn.gsi.CertChainValidatorProvider;
import org.dcache.xrootd.plugins.authn.gsi.CredentialLoader;
import org.dcache.xrootd.plugins.authn.gsi.GSIBucketUtils;
import org.dcache.xrootd.plugins.authn.gsi.GSIBucketUtils.BucketData;
import org.dcache.xrootd.plugins.authn.gsi.GSICredentialManager;
import org.dcache.xrootd.plugins.authn.gsi.post49.GSIPost49ClientRequestHandler;
import org.dcache.xrootd.security.SecurityInfo;
import org.dcache.xrootd.security.SigningPolicy;
import org.dcache.xrootd.security.TLSSessionInfo;
import org.dcache.xrootd.tpc.XrootdTpcClient;
import org.dcache.xrootd.tpc.XrootdTpcInfo;
import org.dcache.xrootd.tpc.protocol.messages.InboundAuthenticationResponse;
import org.dcache.xrootd.util.ServerProtocolFlags;

/**
 * GSI authentication to origins with the service proxy.
 *
 * <p>Uses xrootd4j's client-side GSI handler, written for its third-party-copy
 * client: an {@link XrootdTpcClient} serves only as the context object the
 * handler reads (origin host, security parameters), and the service proxy is
 * passed as its "delegated" proxy. That selects the 4.9+ exchange (signed
 * Diffie-Hellman parameters). The handler checks the origin's certificate chain
 * and that its name matches the origin host.
 */
final class GsiClient {

    private final Optional<ProxyCredential> proxy;
    private final Properties props;
    private final X509CertChainValidator validator;
    private final CredentialLoader loader;

    GsiClient(OriginConfig cfg) throws FileNotFoundException {
        this.proxy = cfg.proxy().map(ProxyCredential::new);
        this.props = GsiProperties.of(cfg.caDir(), null, null);
        this.validator = new CertChainValidatorProvider(props).getCertChainValidator();
        this.loader = new CredentialLoader(props, validator);
    }

    CompletableFuture<Void> authenticate(OriginConnection conn, SecurityInfo gsi) {
        GSIPost49ClientRequestHandler handler;
        ByteBuf certReq;
        try {
            if (proxy.isEmpty()) {
                throw new XrootdException(kXR_NotAuthorized,
                        "origin " + conn + " requires GSI but X509_USER_PROXY is not set");
            }
            XrootdTpcInfo info = new XrootdTpcInfo(Map.of(
                    "tpc.key", "ixcache", "tpc.src", conn.host + ":" + conn.port));
            info.setDelegatedProxy(proxy.get().get());
            XrootdTpcClient context = new XrootdTpcClient(
                    "ixcache." + ProcessHandle.current().pid() + "@ixcache", info, null, null);
            context.setProtocolInfo(gsi);
            context.setSigningPolicy(new SigningPolicy());
            TLSSessionInfo tls = new TLSSessionInfo(new ServerProtocolFlags(0));
            tls.createClientSession(false);
            context.setTlsSessionInfo(tls);

            handler = new GSIPost49ClientRequestHandler(
                    new GSICredentialManager(props, loader, validator), context);
            certReq = Requests.render(handler.handleCertReqStep());
        } catch (Exception e) {
            return CompletableFuture.failedFuture(authError(conn, e));
        }

        return conn.send(certReq).thenCompose(reply -> {
            ByteBuf certStep;
            ByteBuf frame = null;
            InboundAuthenticationResponse inbound;
            try {
                if (reply.status() != kXR_authmore) {
                    throw new XrootdException(kXR_ServerError, "origin " + conn
                            + " ended GSI after the first step");
                }
                frame = frame(reply);
                inbound = new InboundAuthenticationResponse(frame);
                BucketData data = GSIBucketUtils.deserializeData(inbound);
                if (data.getStep() != kXGS_cert) {
                    throw new XrootdException(kXR_ServerError, "origin " + conn
                            + " sent unexpected GSI step " + data.getStep());
                }
                certStep = Requests.render(handler.handleCertStep(inbound, data, null));
            } catch (Exception e) {
                return CompletableFuture.failedFuture(authError(conn, e));
            } finally {
                reply.data().release();
                // xrootd4j releases the inbound message's own copy of the data.
                if (frame != null) {
                    frame.release();
                }
            }
            return conn.send(certStep);
        }).thenAccept(reply -> {
            reply.data().release();
            if (reply.status() != kXR_ok) {
                throw new IllegalStateException("origin " + conn + " asked for more GSI steps than expected");
            }
        });
    }

    /** The reply as the complete frame xrootd4j's message class parses. */
    private static ByteBuf frame(Reply reply) {
        ByteBuf header = Unpooled.buffer(8);
        header.writeShort(0);
        header.writeShort(reply.status());
        header.writeInt(reply.data().readableBytes());
        return Unpooled.wrappedBuffer(header, reply.data().retain());
    }

    private static XrootdException authError(OriginConnection conn, Exception e) {
        if (e instanceof XrootdException x) {
            return x;
        }
        return new XrootdException(kXR_NotAuthorized,
                "GSI authentication to origin " + conn + " failed: " + e.getMessage());
    }
}
