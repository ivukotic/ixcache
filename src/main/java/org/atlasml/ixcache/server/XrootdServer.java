package org.atlasml.ixcache.server;

import static org.dcache.xrootd.core.XrootdSessionHandler.SESSION_HANDLER;
import static org.dcache.xrootd.protocol.XrootdProtocol.DATA_SERVER;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_isServer;

import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.RejectedExecutionException;

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
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import org.atlasml.ixcache.gsi.GsiProperties;
import org.atlasml.ixcache.telemetry.Telemetry;
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
 * The client-facing XRootD server: handshake, login with no authentication or
 * mandatory GSI ({@link ServerConfig.Auth}), then file requests handled by
 * {@link ClientHandler}.
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

    /** Adds the login (and authentication) handler for one connection. */
    @FunctionalInterface
    private interface LoginStage {
        void add(ChannelPipeline p, TLSSessionInfo tls, SigningPolicy signing);
    }

    /** Binds {@code cfg.port()} and starts accepting clients, without telemetry. */
    public static XrootdServer start(ServerConfig cfg, Backend backend)
            throws FileNotFoundException, InterruptedException {
        return start(cfg, backend, null);
    }

    /**
     * Binds {@code cfg.port()} and starts accepting clients.
     *
     * @param telemetry receives a record per closed file; may be null
     */
    public static XrootdServer start(ServerConfig cfg, Backend backend, Telemetry telemetry)
            throws FileNotFoundException, InterruptedException {
        LoginStage login = switch (cfg.auth()) {
            case NONE -> (p, tls, signing) -> p.addLast(SESSION_HANDLER, new AnonymousLoginHandler());
            case GSI -> gsiLogin(cfg);
        };
        log.info("client authentication: {}", cfg.auth().name().toLowerCase(Locale.ROOT));

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
                            initPipeline(ch.pipeline(), login, backend, cfg.site(), telemetry);
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

    /** Mandatory GSI; loads the host credential now so a bad one fails at startup. */
    private static LoginStage gsiLogin(ServerConfig cfg) throws FileNotFoundException {
        checkPatchedGsi();
        Properties gsiProps = GsiProperties.of(cfg.caDir(), cfg.hostCert(), cfg.hostKey());
        AuthenticationFactory gsi = new GSIAuthenticationFactory(gsiProps);
        X509CertChainValidator validator =
                new CertChainValidatorProvider(gsiProps).getCertChainValidator();
        gsi.createHandler(null).getProtocol();

        return (p, tls, signing) -> {
            XrootdSessionHandler session = new XrootdSessionHandler();
            session.setTlsSessionInfo(tls);
            session.setSigningPolicy(signing);
            GsiLoginHandler login = new GsiLoginHandler(gsi, validator);
            login.setSessionHandler(session);
            session.add(login);
            p.addLast(SESSION_HANDLER, session);
        };
    }

    private static void initPipeline(ChannelPipeline p, LoginStage login, Backend backend,
            Optional<String> site, Telemetry telemetry) {
        // Per-connection state: the session and client handlers share one TLSSessionInfo.
        TLSSessionInfo tls = new TLSSessionInfo(new ServerProtocolFlags(kXR_isServer));
        SigningPolicy signing = new SigningPolicy();

        ClientHandler client = new ClientHandler(backend, site, telemetry);
        client.setTlsSessionInfo(tls);
        client.setSigningPolicy(signing);

        p.addLast("handshake", new XrootdHandshakeHandler(DATA_SERVER));
        p.addLast("encoder", new XrootdEncoder());
        p.addLast("decoder", new XrootdDecoder());
        login.add(p, tls, signing);
        p.addLast("client", client);
    }

    public InetSocketAddress localAddress() {
        return (InetSocketAddress) channel.localAddress();
    }

    /** True while the listening socket is open. */
    public boolean isListening() {
        return channel != null && channel.isActive();
    }

    /**
     * True if every client event loop runs a trivial task within {@code timeout}:
     * a blocked or deadlocked loop would stall all its connections.
     */
    public boolean eventLoopsResponsive(Duration timeout) {
        if (workers.isShuttingDown()) {
            return false;
        }
        try {
            for (EventExecutor loop : workers) {
                Future<?> f = loop.submit(() -> { });
                if (!f.awaitUninterruptibly(timeout.toMillis()) || !f.isSuccess()) {
                    return false;
                }
            }
        } catch (RejectedExecutionException e) {
            return false;
        }
        return true;
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
