package org.atlasml.ixcache.origin;

import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_NotAuthorized;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ServerError;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_authmore;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ok;
import static org.dcache.xrootd.security.XrootdSecurityProtocol.kXGS_cert;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import eu.emi.security.authn.x509.ValidationResult;
import eu.emi.security.authn.x509.X509CertChainValidator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.atlasml.ixcache.gsi.GsiProperties;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.plugins.authn.gsi.CertChainValidatorProvider;
import org.dcache.xrootd.plugins.authn.gsi.CredentialLoader;
import org.dcache.xrootd.plugins.authn.gsi.GSIBucket;
import org.dcache.xrootd.plugins.authn.gsi.GSIBucketUtils;
import org.dcache.xrootd.plugins.authn.gsi.GSIBucketUtils.BucketData;
import org.dcache.xrootd.plugins.authn.gsi.GSICredentialManager;
import org.dcache.xrootd.plugins.authn.gsi.post49.GSIPost49ClientRequestHandler;
import org.dcache.xrootd.security.SecurityInfo;
import org.dcache.xrootd.security.XrootdSecurityProtocol.BucketType;
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
 * Diffie-Hellman parameters). The origin's certificate chain must validate and
 * its name must match the URL's host or the reverse-DNS name of the address
 * connected to.
 */
final class GsiClient {

    private static final ExecutorService DNS = Executors.newVirtualThreadPerTaskExecutor();

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

    /** Expiry of the service proxy (re-read if its file changed), or empty if none is configured. */
    Optional<Instant> proxyExpiry() throws IOException {
        if (proxy.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(proxy.get().get().getCertChain()[0].getNotAfter().toInstant());
    }

    CompletableFuture<Void> authenticate(OriginConnection conn, SecurityInfo gsi) {
        // The reverse lookup blocks, so it runs off the event loop.
        return CompletableFuture.supplyAsync(() -> acceptedNames(conn), DNS)
                .thenCompose(names -> authenticate(conn, gsi, names));
    }

    /**
     * Names the origin's certificate may carry: the host in the URL, and the
     * reverse-DNS name of the address connected to. The second is what XRootD
     * clients check by default; it is needed for aliases over several servers
     * (e.g. dCache doors), each with a certificate for its own name.
     */
    private static List<String> acceptedNames(OriginConnection conn) {
        List<String> names = new ArrayList<>(List.of(conn.host));
        InetAddress addr = conn.remoteAddress().getAddress();
        String reverse = addr.getCanonicalHostName();
        if (!reverse.equals(addr.getHostAddress()) && !reverse.equalsIgnoreCase(conn.host)) {
            names.add(reverse);
        }
        return names;
    }

    private CompletableFuture<Void> authenticate(OriginConnection conn, SecurityInfo gsi, List<String> names) {
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

            handler = new Handler(new GSICredentialManager(props, loader, validator), context, names);
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

    /**
     * xrootd4j's handler with a stricter server check: the chain must validate
     * (xrootd4j ignores the validation result) and the certificate must match one
     * of {@code names} (xrootd4j accepts only the host in the URL).
     */
    private final class Handler extends GSIPost49ClientRequestHandler {

        private final List<String> names;

        Handler(GSICredentialManager manager, XrootdTpcClient context, List<String> names) {
            super(manager, context);
            this.names = names;
        }

        @Override
        protected X509Certificate validateCertificate(Map<BucketType, GSIBucket> buckets)
                throws IOException, GeneralSecurityException, XrootdException {
            X509Certificate[] chain = processRSAVerification(buckets, Optional.empty());
            ValidationResult r = validator.validate(chain);
            if (!r.isValid()) {
                throw new GeneralSecurityException("origin certificate chain is not valid: " + r.getErrors());
            }
            for (String name : names) {
                try {
                    GSICredentialManager.checkIdentity(chain[0], name);
                    return chain[0];
                } catch (GeneralSecurityException | UnknownHostException e) {
                    // try the next name
                }
            }
            throw new GeneralSecurityException("origin certificate "
                    + chain[0].getSubjectX500Principal().getName() + " matches none of " + names);
        }
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
