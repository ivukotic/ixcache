package org.atlasml.ixcache.origin;

import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_IOError;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_isDir;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_other;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.atlasml.ixcache.path.RequestPath;
import org.atlasml.ixcache.server.Backend;
import org.atlasml.ixcache.server.FileInfo;
import org.atlasml.ixcache.server.OpenFile;
import org.dcache.xrootd.core.XrootdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads files from the origin named in the client path
 * ({@code /root://origin//path}), with no caching. Redirects are followed;
 * origin errors reach the client with the origin's error code.
 */
public final class OriginBackend implements Backend, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OriginBackend.class);

    private final OriginConfig cfg;
    private final EventLoopGroup group;
    private final OriginPool pool;

    public OriginBackend(OriginConfig cfg) throws FileNotFoundException {
        this.cfg = cfg;
        this.group = new NioEventLoopGroup(Math.max(2, Runtime.getRuntime().availableProcessors() / 4),
                new DefaultThreadFactory("ixcache-origin", true));
        this.pool = new OriginPool(group, cfg, new GsiClient(cfg));
        if (cfg.proxy().isEmpty()) {
            log.warn("X509_USER_PROXY is not set: only origins that need no authentication will work");
        }
    }

    /** Where to open a file: origin, path and the opaque data sent with it. */
    record Target(String host, int port, String path, String cgi) {

        String pathAndCgi() {
            return cgi.isEmpty() ? path : path + "?" + cgi;
        }

        Target redirect(RedirectException r) {
            return new Target(r.host, r.port, path, joinCgi(cgi, r.cgi));
        }

        @Override
        public String toString() {
            return "root://" + host + ":" + port + "/" + path;
        }
    }

    /** An open file handle, valid only on its connection. */
    record Handle(OriginConnection conn, int fh) {

        /** Closes the file at the origin and lets the connection go idle. */
        void release() {
            conn.send(Requests.close(fh)).whenComplete((reply, e) -> {
                if (reply != null) {
                    reply.data().release();
                }
            });
            if (conn.openFiles.decrementAndGet() == 0) {
                conn.markIdle();
            }
        }
    }

    record Opened(Handle handle, FileInfo info) {
    }

    @Override
    public CompletableFuture<OpenFile> open(String path, String opaque) {
        Target t;
        try {
            t = target(path, opaque);
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(e);
        }
        return open(t, 0).thenApply(o -> new OriginFile(this, t, o.info(), o.handle()));
    }

    @Override
    public CompletableFuture<FileInfo> stat(String path, String opaque) {
        Target t;
        try {
            t = target(path, opaque);
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(e);
        }
        return follow(t, 0, (conn, target) -> conn.send(Requests.stat(target.pathAndCgi()))
                .thenApply(reply -> {
                    try {
                        return parseStat(target, reply.data(), reply.data().readerIndex(),
                                reply.data().readableBytes());
                    } finally {
                        reply.data().release();
                    }
                }));
    }

    /** Opens {@code t}, following redirects. */
    CompletableFuture<Opened> open(Target t, int redirects) {
        return follow(t, redirects, (conn, target) -> conn.send(Requests.open(target.pathAndCgi()))
                .thenCompose(reply -> {
                    ByteBuf d = reply.data();
                    int fh;
                    FileInfo info = null;
                    try {
                        fh = d.getInt(d.readerIndex());
                        // fhandle, then (with kXR_retstat) cpsize, cptype and the stat string
                        if (d.readableBytes() > 12) {
                            info = parseStat(target, d, d.readerIndex() + 12, d.readableBytes() - 12);
                        }
                    } finally {
                        d.release();
                    }
                    conn.openFiles.incrementAndGet();
                    Handle h = new Handle(conn, fh);
                    if (info != null) {
                        return CompletableFuture.completedFuture(new Opened(h, info));
                    }
                    // The origin ignored kXR_retstat: ask separately.
                    return conn.send(Requests.stat(target.pathAndCgi())).thenApply(r -> {
                        try {
                            return new Opened(h, parseStat(target, r.data(), r.data().readerIndex(),
                                    r.data().readableBytes()));
                        } finally {
                            r.data().release();
                        }
                    }).whenComplete((o, e) -> {
                        if (e != null) {
                            h.release();
                        }
                    });
                }));
    }

    /** Runs {@code op} against {@code t}, retrying at each redirect target. */
    private <T> CompletableFuture<T> follow(Target t, int redirects,
            BiFunction<OriginConnection, Target, CompletableFuture<T>> op) {
        return pool.acquire(t.host(), t.port())
                .thenCompose(conn -> op.apply(conn, t))
                .exceptionallyCompose(e -> {
                    Throwable cause = OriginConnection.unwrap(e);
                    if (cause instanceof RedirectException r) {
                        if (redirects >= cfg.maxRedirects()) {
                            return CompletableFuture.failedFuture(new XrootdException(kXR_IOError,
                                    "too many redirects opening " + t));
                        }
                        Target next = t.redirect(r);
                        log.debug("{} redirected to {}:{}", t, r.host, r.port);
                        return follow(next, redirects + 1, op);
                    }
                    return CompletableFuture.failedFuture(cause);
                });
    }

    private static Target target(String clientPath, String clientOpaque) {
        RequestPath rp = RequestPath.parse(clientPath);
        return new Target(rp.host(), rp.port(), rp.path(), joinCgi(rp.opaque(), forwardable(clientOpaque)));
    }

    /** Parses {@code "id size flags mtime"}. */
    private static FileInfo parseStat(Target t, ByteBuf b, int index, int length) {
        String s = OriginConnection.cString(b, index, length).trim();
        String[] f = s.split("\\s+");
        if (f.length < 4) {
            throw new CompletionException(new IOException("bad stat reply from origin for " + t + ": '" + s + "'"));
        }
        int flags = Integer.parseInt(f[2]);
        if ((flags & (kXR_isDir | kXR_other)) != 0) {
            throw new CompletionException(new NoSuchFileException(t.path(), null, "not a regular file"));
        }
        return new FileInfo(Long.parseLong(f[1]), Long.parseLong(f[3]));
    }

    /**
     * The client's opaque data minus what the client meant for ixcache itself
     * ({@code xrdcl.*}, {@code xrd.*}, and its retry hints {@code tried}/{@code triedrc}).
     */
    static String forwardable(String opaque) {
        if (opaque == null || opaque.isEmpty()) {
            return "";
        }
        List<String> keep = new ArrayList<>();
        for (String kv : opaque.split("&")) {
            String key = kv.contains("=") ? kv.substring(0, kv.indexOf('=')) : kv;
            if (kv.isEmpty() || key.startsWith("xrdcl.") || key.startsWith("xrd.")
                    || key.equals("tried") || key.equals("triedrc")) {
                continue;
            }
            keep.add(kv);
        }
        return String.join("&", keep);
    }

    static String joinCgi(String a, String b) {
        if (a == null || a.isEmpty()) {
            return b == null ? "" : b;
        }
        if (b == null || b.isEmpty()) {
            return a;
        }
        return a + "&" + b;
    }

    @Override
    public void close() {
        pool.close();
        group.shutdownGracefully();
    }
}
