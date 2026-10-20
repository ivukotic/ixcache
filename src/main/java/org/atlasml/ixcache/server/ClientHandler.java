package org.atlasml.ixcache.server;

import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ArgInvalid;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ArgMissing;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ArgTooLong;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_FSError;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_FileNotOpen;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_IOError;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_NotAuthorized;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_NotFound;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_Qconfig;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ServerError;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_readable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import org.atlasml.ixcache.cache.Range;
import org.atlasml.ixcache.path.RequestPath;
import org.atlasml.ixcache.telemetry.Telemetry;
import org.atlasml.ixcache.telemetry.TransferRecord;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.core.XrootdProtocolRequestHandler;
import org.dcache.xrootd.protocol.messages.CloseRequest;
import org.dcache.xrootd.protocol.messages.GenericReadRequestMessage.EmbeddedReadRequest;
import org.dcache.xrootd.protocol.messages.OkResponse;
import org.dcache.xrootd.protocol.messages.OpenRequest;
import org.dcache.xrootd.protocol.messages.OpenResponse;
import org.dcache.xrootd.protocol.messages.PingRequest;
import org.dcache.xrootd.protocol.messages.QueryRequest;
import org.dcache.xrootd.protocol.messages.QueryResponse;
import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.protocol.messages.ReadResponse;
import org.dcache.xrootd.protocol.messages.ReadVRequest;
import org.dcache.xrootd.protocol.messages.ReadVResponse;
import org.dcache.xrootd.protocol.messages.StatRequest;
import org.dcache.xrootd.protocol.messages.StatResponse;
import org.dcache.xrootd.protocol.messages.XrootdRequest;
import org.dcache.xrootd.util.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles file requests on one authenticated client connection.
 *
 * <p>Requests that need the backend return {@code null} to xrootd4j and answer
 * later from the backend's completion, so many requests can be outstanding on a
 * connection at once; replies are matched to requests by stream ID. All state is
 * touched only on the channel's event loop.
 */
final class ClientHandler extends XrootdProtocolRequestHandler {

    private static final Logger log = LoggerFactory.getLogger(ClientHandler.class);

    /** Largest single read or vector read reply. Clients read in chunks well below this. */
    static final int MAX_READ_BYTES = 256 * 1024 * 1024;
    /** Vector read limits advertised through kXR_Qconfig, as in the reference server. */
    static final int READV_IOV_MAX = 1024;
    static final int READV_IOR_MAX = 2097136;

    private final Backend backend;
    private final Optional<String> site;
    private final Telemetry telemetry; // may be null
    private final Map<Integer, Open> files = new HashMap<>();
    private int nextHandle;
    private ChannelHandlerContext ctx;

    /** A file this client has open, with what it did, for telemetry. */
    private static final class Open {
        final OpenFile file;
        final String path;
        final long openedNanos = System.nanoTime();
        long readOps;
        long readvOps;
        long readvElements;
        long bytesOut;
        long errors;

        Open(OpenFile file, String path) {
            this.file = file;
            this.path = path;
        }
    }

    ClientHandler(Backend backend, Optional<String> site, Telemetry telemetry) {
        this.backend = backend;
        this.site = site;
        this.telemetry = telemetry;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        for (Open o : files.values()) {
            o.file.close();
            report(o, "disconnected");
        }
        files.clear();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof IOException) {
            log.debug("{} connection error: {}", ctx.channel().remoteAddress(), cause.toString());
        } else {
            log.warn("{} closing after unexpected error", ctx.channel().remoteAddress(), cause);
        }
        ctx.close();
    }

    @Override
    protected Object doOnOpen(ChannelHandlerContext ctx, OpenRequest msg) throws XrootdException {
        if (msg.isNew() || msg.isDelete() || msg.isReadWrite() || msg.isAppend()) {
            throw new XrootdException(kXR_NotAuthorized, "ixcache is read-only");
        }
        backend.open(msg.getPath(), msg.getOpaque()).whenComplete((file, err) -> onLoop(() -> {
            if (err != null) {
                fail(msg, err);
            } else if (!ctx.channel().isActive()) {
                file.close();
            } else {
                int fh = nextHandle++;
                files.put(fh, new Open(file, msg.getPath()));
                FileStatus fs = msg.isRetStat() ? status(file.info()) : null;
                respond(ctx, new OpenResponse(msg, fh, null, null, fs));
            }
        }));
        return null;
    }

    @Override
    protected Object doOnStat(ChannelHandlerContext ctx, StatRequest msg) throws XrootdException {
        if (msg.getTarget() == StatRequest.Target.FHANDLE) {
            return new StatResponse(msg, status(open(msg.getFhandle()).file.info()));
        }
        reply(msg, backend.stat(msg.getPath(), msg.getOpaque()),
                info -> new StatResponse(msg, status(info)));
        return null;
    }

    @Override
    protected Object doOnRead(ChannelHandlerContext ctx, ReadRequest msg) throws XrootdException {
        Open o = open(msg.getFileHandle());
        if (msg.getReadOffset() < 0 || msg.bytesToRead() < 0) {
            throw new XrootdException(kXR_ArgInvalid, "negative offset or length");
        }
        if (msg.bytesToRead() > MAX_READ_BYTES) {
            throw new XrootdException(kXR_ArgTooLong, "read larger than " + MAX_READ_BYTES);
        }
        Range r = new Range(msg.getReadOffset(), msg.bytesToRead()).clamp(o.file.info().size());
        o.readOps++;
        if (r.length() == 0) {
            return new ReadResponse(msg, Unpooled.EMPTY_BUFFER, false);
        }
        reply(msg, o.file.readv(List.of(r)),
                data -> new ReadResponse(msg, Unpooled.wrappedBuffer(data.get(0)), false),
                err -> {
                    if (err == null) {
                        o.bytesOut += r.length();
                    } else {
                        o.errors++;
                    }
                });
        return null;
    }

    @Override
    protected Object doOnReadV(ChannelHandlerContext ctx, ReadVRequest msg) throws XrootdException {
        EmbeddedReadRequest[] reqs = msg.getReadRequestList();
        if (reqs.length == 0) {
            throw new XrootdException(kXR_ArgMissing, "empty vector read");
        }
        if (reqs.length > READV_IOV_MAX) {
            throw new XrootdException(kXR_ArgTooLong, "more than " + READV_IOV_MAX + " elements");
        }

        // Elements may name different files: one backend read per file.
        Map<Open, List<Integer>> byFile = new LinkedHashMap<>();
        Range[] ranges = new Range[reqs.length];
        long total = 0;
        for (int i = 0; i < reqs.length; i++) {
            EmbeddedReadRequest e = reqs[i];
            if (e.getOffset() < 0 || e.BytesToRead() < 0) {
                throw new XrootdException(kXR_ArgInvalid, "negative offset or length");
            }
            Open o = open(e.getFileHandle());
            ranges[i] = new Range(e.getOffset(), e.BytesToRead());
            // Clients require every element in full; the reference server fails the
            // whole request when one extends past EOF, and so do we.
            if (ranges[i].end() > o.file.info().size()) {
                throw new XrootdException(kXR_FSError, "vector read past end of file");
            }
            total += ranges[i].length();
            byFile.computeIfAbsent(o, f -> new ArrayList<>()).add(i);
        }
        if (total > MAX_READ_BYTES) {
            throw new XrootdException(kXR_ArgTooLong, "vector read larger than " + MAX_READ_BYTES);
        }

        ByteBuf[] data = new ByteBuf[reqs.length];
        List<CompletableFuture<Void>> parts = new ArrayList<>(byFile.size());
        byFile.forEach((o, idx) -> {
            o.readvOps++;
            o.readvElements += idx.size();
            List<Range> rs = idx.stream().map(i -> ranges[i]).toList();
            parts.add(o.file.readv(rs).thenAccept(bufs -> {
                for (int k = 0; k < idx.size(); k++) {
                    data[idx.get(k)] = Unpooled.wrappedBuffer(bufs.get(k));
                }
            }));
        });
        reply(msg, CompletableFuture.allOf(parts.toArray(CompletableFuture[]::new)),
                v -> new ReadVResponse(msg, reqs, data, 0, reqs.length, false),
                err -> byFile.forEach((o, idx) -> {
                    if (err == null) {
                        idx.forEach(i -> o.bytesOut += ranges[i].length());
                    } else {
                        o.errors++;
                    }
                }));
        return null;
    }

    @Override
    protected Object doOnClose(ChannelHandlerContext ctx, CloseRequest msg) throws XrootdException {
        Open o = files.remove(msg.getFileHandle());
        if (o == null) {
            throw new XrootdException(kXR_FileNotOpen, "invalid file handle");
        }
        o.file.close();
        report(o, "closed");
        return withOk(msg);
    }

    @Override
    protected Object doOnQuery(ChannelHandlerContext ctx, QueryRequest msg) throws XrootdException {
        if (msg.getReqcode() != kXR_Qconfig) {
            return unsupported(ctx, msg);
        }
        // One line per requested name; unknown names are echoed back, as xrootd does.
        StringBuilder sb = new StringBuilder();
        for (String name : msg.getArgs().trim().split("\\s+")) {
            sb.append(switch (name) {
                case "readv_iov_max" -> String.valueOf(READV_IOV_MAX);
                case "readv_ior_max" -> String.valueOf(READV_IOR_MAX);
                case "sitename" -> site.orElse(name);
                default -> name;
            }).append('\n');
        }
        return new QueryResponse(msg, sb.toString());
    }

    @Override
    protected Object doOnPing(ChannelHandlerContext ctx, PingRequest msg) {
        return new OkResponse<>(msg);
    }

    private Open open(int fh) throws XrootdException {
        Open o = files.get(fh);
        if (o == null) {
            throw new XrootdException(kXR_FileNotOpen, "invalid file handle");
        }
        return o;
    }

    /** Answers {@code msg} on the event loop when {@code f} completes. */
    private <T, R extends XrootdRequest> void reply(R msg, CompletableFuture<T> f,
            Function<T, Object> response) {
        reply(msg, f, response, err -> { });
    }

    /** As above; {@code done} then runs on the event loop with the error, or null on success. */
    private <T, R extends XrootdRequest> void reply(R msg, CompletableFuture<T> f,
            Function<T, Object> response, Consumer<Throwable> done) {
        f.whenComplete((v, err) -> onLoop(() -> {
            done.accept(err);
            if (err != null) {
                fail(msg, err);
            } else if (ctx.channel().isActive()) {
                respond(ctx, response.apply(v));
            }
        }));
    }

    /** Sends the telemetry record for a file this client no longer has open. */
    private void report(Open o, String status) {
        if (telemetry == null) {
            return;
        }
        String origin = "";
        String path = o.path;
        String key = "";
        try {
            RequestPath rp = RequestPath.parse(o.path);
            origin = rp.host() + ":" + rp.port();
            path = rp.path();
            key = rp.cacheKey().id();
        } catch (IllegalArgumentException e) {
            // not an embedded origin URL (local files): report the path as given
        }
        String host = ctx.channel().remoteAddress() instanceof InetSocketAddress a
                ? a.getAddress().getHostAddress() : String.valueOf(ctx.channel().remoteAddress());
        String dn = ctx.channel().attr(GsiLoginHandler.CLIENT_DN).get();
        telemetry.record(new TransferRecord(host, dn == null ? "" : dn, origin, path, key,
                o.file.info().size(), o.bytesOut, o.file.bytesFromDisk(), o.file.bytesFromOrigin(),
                o.readOps, o.readvOps, o.readvElements, o.errors,
                (System.nanoTime() - o.openedNanos) / 1_000_000, status));
    }

    private void onLoop(Runnable r) {
        if (ctx.executor().inEventLoop()) {
            r.run();
        } else {
            ctx.executor().execute(r);
        }
    }

    private void fail(XrootdRequest msg, Throwable err) {
        while ((err instanceof CompletionException || err instanceof UncheckedIOException)
                && err.getCause() != null) {
            err = err.getCause();
        }
        // Messages for I/O errors are generic: they could name paths on this host.
        int code;
        String text;
        if (err instanceof XrootdException x) {
            code = x.getError();
            text = x.getMessage();
        } else if (err instanceof NoSuchFileException) {
            code = kXR_NotFound;
            text = "no such file or directory";
        } else if (err instanceof IllegalArgumentException) {
            code = kXR_ArgInvalid;
            text = err.getMessage();
        } else if (err instanceof IOException) {
            code = kXR_IOError;
            text = "I/O error";
        } else {
            log.error("{} failed", msg, err);
            code = kXR_ServerError;
            text = "internal server error";
        }
        log.debug("{} failed: {}", msg, err.toString());
        if (ctx.channel().isActive()) {
            respond(ctx, withError(ctx, msg, code, text));
        }
    }

    private static FileStatus status(FileInfo info) {
        return new FileStatus(0, info.size(), kXR_readable, info.modTime());
    }

}
