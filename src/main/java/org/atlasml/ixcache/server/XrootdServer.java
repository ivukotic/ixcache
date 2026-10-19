package org.atlasml.ixcache.server;

import static org.dcache.xrootd.core.XrootdSessionHandler.SESSION_HANDLER;
import static org.dcache.xrootd.protocol.XrootdProtocol.DATA_SERVER;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_isServer;

import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.Properties;

import eu.emi.security.authn.x509.X509CertChainValidator;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.dcache.xrootd.core.XrootdDecoder;
import org.dcache.xrootd.core.XrootdEncoder;
import org.dcache.xrootd.core.XrootdHandshakeHandler;
import org.dcache.xrootd.core.XrootdSessionHandler;
import org.dcache.xrootd.plugins.AuthenticationFactory;
import org.dcache.xrootd.plugins.authn.gsi.CertChainValidatorProvider;
import org.dcache.xrootd.plugins.authn.gsi.GSIAuthenticationFactory;
import org.dcache.xrootd.plugins.authn.gsi.GSIRequestHandler;
import org.dcache.xrootd.security.SigningPolicy;
import org.dcache.xrootd.security.TLSSessionInfo;
import org.dcache.xrootd.util.ServerProtocolFlags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The client-facing XRootD server: handshake, login, mandatory GSI
 * authentication, then file requests handled by {@link ClientHandler}.
 *
 * <p>No TLS and no request signing. The server does not advertise
 * {@code kXR_pgread} support, so clients use plain {@code kXR_read}.
 */
public final class XrootdServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(XrootdServer.class);

    private final EventLoopGroup boss = new NioEventLoopGroup(1);
    private final EventLoopGroup workers = new NioEventLoopGroup();
    private Channel channel;

    private XrootdServer() {
    }

    /** Binds {@code cfg.port()} and starts accepting clients. */
    public static XrootdServer start(ServerConfig cfg, Backend backend)
            throws FileNotFoundException, InterruptedException {
        checkPatchedGsi();
        Properties gsiProps = gsiProperties(cfg);
        AuthenticationFactory gsi = new GSIAuthenticationFactory(gsiProps);
        X509CertChainValidator validator =
                new CertChainValidatorProvider(gsiProps).getCertChainValidator();
        // Load the host credential now so a bad certificate fails at startup, not at first login.
        gsi.createHandler(null).getProtocol();

        XrootdServer s = new XrootdServer();
        try {
            s.channel = new ServerBootstrap()
                    .group(s.boss, s.workers)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            initPipeline(ch.pipeline(), gsi, validator, backend);
                        }
                    })
                    .bind(new InetSocketAddress(cfg.port())).sync().channel();
        } catch (RuntimeException | InterruptedException e) {
            s.close();
            throw e;
        }
        log.info("listening on {}", s.channel.localAddress());
        return s;
    }

    /**
     * Fails unless ixcache's patched {@link GSIRequestHandler} is the one loaded.
     * The stock class sends random tags that XRootD 6 clients refuse, and signs any
     * tag a client sends; it wins if xrootd4j-gsi precedes ixcache on the classpath.
     */
    static void checkPatchedGsi() {
        try {
            if (GSIRequestHandler.isWellFormedChallenge(GSIRequestHandler.generateChallengeString())) {
                return;
            }
        } catch (LinkageError e) {
            // stock class: the method does not exist
        }
        throw new IllegalStateException("unpatched xrootd4j GSIRequestHandler loaded from "
                + GSIRequestHandler.class.getProtectionDomain().getCodeSource().getLocation()
                + "; put the ixcache jar before xrootd4j-gsi on the classpath");
    }

    private static void initPipeline(ChannelPipeline p, AuthenticationFactory gsi,
            X509CertChainValidator validator, Backend backend) {
        // Per-connection state: both handlers must share one TLSSessionInfo.
        TLSSessionInfo tls = new TLSSessionInfo(new ServerProtocolFlags(kXR_isServer));
        SigningPolicy signing = new SigningPolicy();

        XrootdSessionHandler session = new XrootdSessionHandler();
        session.setTlsSessionInfo(tls);
        session.setSigningPolicy(signing);
        GsiLoginHandler login = new GsiLoginHandler(gsi, validator);
        login.setSessionHandler(session);
        session.add(login);

        ClientHandler client = new ClientHandler(backend);
        client.setTlsSessionInfo(tls);
        client.setSigningPolicy(signing);

        p.addLast("handshake", new XrootdHandshakeHandler(DATA_SERVER));
        p.addLast("encoder", new XrootdEncoder());
        p.addLast("decoder", new XrootdDecoder());
        p.addLast(SESSION_HANDLER, session);
        p.addLast("client", client);
    }

    private static Properties gsiProperties(ServerConfig cfg) {
        Properties p = new Properties();
        p.setProperty("xrootd.gsi.hostcert.cert", cfg.hostCert().toString());
        p.setProperty("xrootd.gsi.hostcert.key", cfg.hostKey().toString());
        p.setProperty("xrootd.gsi.hostcert.refresh", "1");
        p.setProperty("xrootd.gsi.hostcert.refresh.unit", "HOURS");
        p.setProperty("xrootd.gsi.hostcert.verify", "true");
        p.setProperty("xrootd.gsi.ca.path", cfg.caDir().toString());
        p.setProperty("xrootd.gsi.ca.refresh", "1");
        p.setProperty("xrootd.gsi.ca.refresh.unit", "HOURS");
        p.setProperty("xrootd.gsi.ca.namespace-mode", "EUGRIDPMA_GLOBUS");
        p.setProperty("xrootd.gsi.ca.crl-mode", "IF_VALID");
        p.setProperty("xrootd.gsi.ca.ocsp-mode", "IGNORE");
        // Client-side (TPC) settings are unused but must parse.
        p.setProperty("xrootd.gsi.tpc.cred.refresh", "1");
        p.setProperty("xrootd.gsi.tpc.cred.refresh.unit", "HOURS");
        p.setProperty("xrootd.gsi.tpc.cred.verify", "false");
        return p;
    }

    public InetSocketAddress localAddress() {
        return (InetSocketAddress) channel.localAddress();
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.close().syncUninterruptibly();
        }
        boss.shutdownGracefully();
        workers.shutdownGracefully();
    }
}
