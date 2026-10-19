package org.atlasml.ixcache.origin;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.dcache.xrootd.protocol.XrootdProtocol.HANDSHAKE_REQUEST;
import static org.dcache.xrootd.protocol.XrootdProtocol.SESSION_ID_SIZE;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_IOError;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ServerError;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_asynresp;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_attn;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_authmore;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_error;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ok;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_oksofar;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_redirect;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_wait;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_waitresp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.security.SecurityInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One logged-in (and, if the origin asks for it, GSI-authenticated) connection
 * to an origin, carrying many requests at once. Replies are matched to requests
 * by stream ID.
 *
 * <p>Errors complete the request's future with {@link XrootdException} (the
 * origin's error code), redirects with {@link RedirectException}, and a lost
 * connection with {@link ConnectionLostException}. {@code kXR_wait} is retried
 * here. All state except the counters is touched only on the channel's event loop.
 */
final class OriginConnection {

    private static final Logger log = LoggerFactory.getLogger(OriginConnection.class);

    /** Largest reply frame accepted; ixcache never asks for more than this in one request. */
    static final int MAX_FRAME = 64 * 1024 * 1024 + 8;
    private static final int MAX_WAIT_SECONDS = 60;

    final String host;
    final int port;
    private final OriginConfig cfg;
    private final Channel channel;
    private final AtomicInteger outstanding = new AtomicInteger();
    /** Files open through this connection; their handles are valid only here. */
    final AtomicInteger openFiles = new AtomicInteger();
    private volatile long idleSinceNanos = System.nanoTime();

    // Event loop only.
    private final Map<Integer, Pending> pending = new HashMap<>();
    private int nextStream = 1;
    private boolean closed;

    private static final class Pending {
        final ByteBuf request; // kept for kXR_wait resends
        final CompletableFuture<Reply> result = new CompletableFuture<>();
        CompositeByteBuf data;
        ScheduledFuture<?> timer;
        int waits;
        boolean abandoned; // timed out: the stream ID stays reserved until the reply comes

        Pending(ByteBuf request) {
            this.request = request;
        }
    }

    /** Thrown when the connection closes with requests outstanding. */
    static final class ConnectionLostException extends IOException {
        private static final long serialVersionUID = 1L;

        ConnectionLostException(String message) {
            super(message);
        }
    }

    private OriginConnection(String host, int port, OriginConfig cfg, Channel channel) {
        this.host = host;
        this.port = port;
        this.cfg = cfg;
        this.channel = channel;
        channel.pipeline().addLast(new Handler());
    }

    /** Connects, logs in and authenticates. */
    static CompletableFuture<OriginConnection> connect(EventLoopGroup group, String host, int port,
            OriginConfig cfg, GsiClient gsi) {
        CompletableFuture<OriginConnection> f = new CompletableFuture<>();
        new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) cfg.connectTimeout().toMillis())
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME, 4, 4, 0, 0));
                    }
                })
                .connect(host, port)
                .addListener((ChannelFuture cf) -> {
                    if (!cf.isSuccess()) {
                        f.completeExceptionally(new IOException(
                                "cannot connect to origin " + host + ":" + port + ": " + cf.cause().getMessage(),
                                cf.cause()));
                        return;
                    }
                    OriginConnection c = new OriginConnection(host, port, cfg, cf.channel());
                    c.login(gsi).whenComplete((v, err) -> {
                        if (err != null) {
                            c.close();
                            f.completeExceptionally(unwrap(err));
                        } else {
                            log.debug("connected to origin {}", c);
                            f.complete(c);
                        }
                    });
                });
        return f;
    }

    private CompletableFuture<Void> login(GsiClient gsi) {
        // The server answers the handshake on stream 0, then the kXR_protocol request.
        CompletableFuture<Reply> hs = send(Unpooled.copiedBuffer(HANDSHAKE_REQUEST), 0);
        CompletableFuture<Reply> proto = send(Requests.protocol());
        return hs.thenCombine(proto, (a, b) -> {
                    a.data().release();
                    b.data().release();
                    return null;
                })
                .thenCompose(v -> send(Requests.login("ixcache", (int) ProcessHandle.current().pid())))
                .thenCompose(reply -> {
                    List<SecurityInfo> protocols;
                    try {
                        protocols = securityProtocols(reply.data());
                    } catch (XrootdException e) {
                        return CompletableFuture.failedFuture(e);
                    } finally {
                        reply.data().release();
                    }
                    if (protocols.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    for (SecurityInfo p : protocols) {
                        if (p.getProtocol().equals("gsi")) {
                            return gsi.authenticate(this, p);
                        }
                    }
                    return CompletableFuture.failedFuture(new XrootdException(kXR_ServerError,
                            "origin " + this + " requires authentication ixcache does not support: " + protocols));
                });
    }

    /** Parses the security part of a login reply: {@code &P=gsi,v:...&P=unix}. */
    private static List<SecurityInfo> securityProtocols(ByteBuf data) throws XrootdException {
        List<SecurityInfo> out = new ArrayList<>();
        if (data.readableBytes() <= SESSION_ID_SIZE) {
            return out;
        }
        String sec = cString(data, SESSION_ID_SIZE, data.readableBytes() - SESSION_ID_SIZE);
        for (String part : sec.split("&")) {
            part = part.trim();
            if (part.startsWith("P=")) {
                out.add(new SecurityInfo(part.substring(2)));
            }
        }
        return out;
    }

    /** Sends a request (header stream ID is set here) and returns its complete reply. */
    CompletableFuture<Reply> send(ByteBuf request) {
        return send(request, -1);
    }

    private CompletableFuture<Reply> send(ByteBuf request, int fixedStream) {
        CompletableFuture<Reply> f = new CompletableFuture<>();
        channel.eventLoop().execute(() -> {
            if (closed || !channel.isActive()) {
                request.release();
                f.completeExceptionally(new ConnectionLostException("connection to origin " + this + " is closed"));
                return;
            }
            int sid = fixedStream >= 0 ? fixedStream : allocateStream();
            if (sid < 0) {
                request.release();
                f.completeExceptionally(new XrootdException(kXR_ServerError, "no free stream IDs"));
                return;
            }
            request.setShort(0, sid);
            Pending p = new Pending(request.retainedDuplicate());
            pending.put(sid, p);
            outstanding.incrementAndGet();
            p.result.whenComplete((r, e) -> {
                if (e != null) {
                    f.completeExceptionally(e);
                } else {
                    f.complete(r);
                }
            });
            arm(sid, p, cfg.requestTimeout().toSeconds());
            channel.writeAndFlush(request);
        });
        return f;
    }

    private int allocateStream() {
        for (int i = 0; i < 65535; i++) {
            int sid = nextStream;
            nextStream = nextStream == 65535 ? 1 : nextStream + 1;
            if (!pending.containsKey(sid)) {
                return sid;
            }
        }
        return -1;
    }

    private void arm(int sid, Pending p, long seconds) {
        if (p.timer != null) {
            p.timer.cancel(false);
        }
        p.timer = channel.eventLoop().schedule(() -> timeout(sid, p), seconds, TimeUnit.SECONDS);
    }

    private void timeout(int sid, Pending p) {
        if (p.result.isDone()) {
            return;
        }
        p.abandoned = true;
        releaseData(p);
        settled();
        p.result.completeExceptionally(new XrootdException(kXR_IOError,
                "no reply from origin " + this + " within " + cfg.requestTimeout().toSeconds() + "s"));
    }

    private void onFrame(ByteBuf frame) {
        int sid = frame.getUnsignedShort(0);
        int status = frame.getUnsignedShort(2);
        int dlen = frame.getInt(4);
        ByteBuf body = frame.slice(8, dlen);

        if (status == kXR_attn) {
            // An asynchronous reply to a request answered earlier with kXR_waitresp
            // carries a complete reply frame after an 8-byte prefix.
            if (dlen >= 16 && body.getInt(0) == kXR_asynresp) {
                onFrame(body.slice(8, dlen - 8));
            }
            return;
        }

        Pending p = pending.get(sid);
        if (p == null) {
            log.debug("{}: reply for unknown stream {}", this, sid);
            return;
        }
        if (p.abandoned) {
            if (status != kXR_oksofar) {
                finish(sid, p);
            }
            return;
        }

        switch (status) {
            case kXR_ok, kXR_authmore -> {
                ByteBuf data;
                if (p.data == null) {
                    data = body.retain();
                } else {
                    p.data.addComponent(true, body.retain());
                    data = p.data;
                    p.data = null;
                }
                finish(sid, p);
                settled();
                p.result.complete(new Reply(status, data));
            }
            case kXR_oksofar -> {
                if (p.data == null) {
                    p.data = channel.alloc().compositeBuffer(Integer.MAX_VALUE);
                }
                p.data.addComponent(true, body.retain());
                arm(sid, p, cfg.requestTimeout().toSeconds());
            }
            case kXR_error -> fail(sid, p, new XrootdException(body.getInt(0),
                    "origin " + this + ": " + cString(body, 4, dlen - 4)));
            case kXR_redirect -> fail(sid, p, RedirectException.parse(body.getInt(0),
                    cString(body, 4, dlen - 4)));
            case kXR_wait -> {
                int seconds = Math.min(Math.max(body.getInt(0), 1), MAX_WAIT_SECONDS);
                if (++p.waits > cfg.maxWaits()) {
                    fail(sid, p, new XrootdException(kXR_IOError, "origin " + this + " kept asking to wait"));
                    return;
                }
                log.debug("{}: waiting {}s before resending stream {}", this, seconds, sid);
                arm(sid, p, seconds + cfg.requestTimeout().toSeconds());
                channel.eventLoop().schedule(() -> {
                    if (pending.get(sid) == p && !p.result.isDone() && channel.isActive()) {
                        channel.writeAndFlush(p.request.retainedDuplicate());
                    }
                }, seconds, TimeUnit.SECONDS);
            }
            case kXR_waitresp -> arm(sid, p, Math.max(body.getInt(0), 0) + cfg.requestTimeout().toSeconds());
            default -> fail(sid, p, new XrootdException(kXR_ServerError,
                    "origin " + this + " sent unexpected status " + status));
        }
    }

    private void fail(int sid, Pending p, Throwable t) {
        releaseData(p);
        finish(sid, p);
        settled();
        p.result.completeExceptionally(t);
    }

    /** Frees the stream ID and the kept request. */
    private void finish(int sid, Pending p) {
        pending.remove(sid);
        if (p.timer != null) {
            p.timer.cancel(false);
        }
        p.request.release();
    }

    private void settled() {
        if (outstanding.decrementAndGet() == 0) {
            idleSinceNanos = System.nanoTime();
        }
    }

    private static void releaseData(Pending p) {
        if (p.data != null) {
            p.data.release();
            p.data = null;
        }
    }

    private void closeAll() {
        closed = true;
        ConnectionLostException lost = new ConnectionLostException("connection to origin " + this + " closed");
        for (Map.Entry<Integer, Pending> e : List.copyOf(pending.entrySet())) {
            Pending p = e.getValue();
            if (p.abandoned) {
                finish(e.getKey(), p);
            } else {
                fail(e.getKey(), p, lost);
            }
        }
    }

    boolean isActive() {
        return channel.isActive();
    }

    /** Requests waiting for a reply. */
    int outstanding() {
        return outstanding.get();
    }

    /** Nanoseconds since the connection last had nothing to do, or 0 if it is busy. */
    long idleNanos() {
        return outstanding.get() > 0 || openFiles.get() > 0 ? 0 : System.nanoTime() - idleSinceNanos;
    }

    void markIdle() {
        idleSinceNanos = System.nanoTime();
    }

    void close() {
        channel.close();
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    /** A NUL-terminated (or length-bounded) ASCII string. */
    static String cString(ByteBuf b, int index, int length) {
        if (length <= 0) {
            return "";
        }
        int end = b.indexOf(index, index + length, (byte) 0);
        return b.toString(index, (end < 0 ? index + length : end) - index, US_ASCII);
    }

    static Throwable unwrap(Throwable t) {
        while (t instanceof CompletionException && t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }

    private final class Handler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf frame = (ByteBuf) msg;
            try {
                onFrame(frame);
            } catch (RuntimeException e) {
                log.warn("{}: malformed reply, closing connection", OriginConnection.this, e);
                ctx.close();
            } finally {
                frame.release();
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            log.debug("connection to origin {} closed", OriginConnection.this);
            closeAll();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.debug("connection to origin {} failed: {}", OriginConnection.this, cause.toString());
            ctx.close();
        }
    }
}
