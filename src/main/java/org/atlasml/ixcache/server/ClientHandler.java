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
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import org.atlasml.ixcache.cache.Range;
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
    private final Map<Integer, OpenFile> files = new HashMap<>();
    private int nextHandle;
    private ChannelHandlerContext ctx;

    ClientHandler(Backend backend) {
        this.backend = backend;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        files.values().forEach(OpenFile::close);
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
                files.put(fh, file);
                FileStatus fs = msg.isRetStat() ? status(file.info()) : null;
                respond(ctx, new OpenResponse(msg, fh, null, null, fs));
            }
        }));
        return null;
    }

    @Override
    protected Object doOnStat(ChannelHandlerContext ctx, StatRequest msg) throws XrootdException {
        if (msg.getTarget() == StatRequest.Target.FHANDLE) {
            return new StatResponse(msg, status(file(msg.getFhandle()).info()));
        }
        reply(msg, backend.stat(msg.getPath(), msg.getOpaque()),
                info -> new StatResponse(msg, status(info)));
        return null;
    }

    @Override
    protected Object doOnRead(ChannelHandlerContext ctx, ReadRequest msg) throws XrootdException {
        OpenFile file = file(msg.getFileHandle());
        if (msg.getReadOffset() < 0 || msg.bytesToRead() < 0) {
            throw new XrootdException(kXR_ArgInvalid, "negative offset or length");
        }
        if (msg.bytesToRead() > MAX_READ_BYTES) {
            throw new XrootdException(kXR_ArgTooLong, "read larger than " + MAX_READ_BYTES);
        }
        Range r = new Range(msg.getReadOffset(), msg.bytesToRead()).clamp(file.info().size());
        if (r.length() == 0) {
            return new ReadResponse(msg, Unpooled.EMPTY_BUFFER, false);
        }
        reply(msg, file.readv(List.of(r)),
                data -> new ReadResponse(msg, Unpooled.wrappedBuffer(data.get(0)), false));
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
        Map<OpenFile, List<Integer>> byFile = new LinkedHashMap<>();
        Range[] ranges = new Range[reqs.length];
        long total = 0;
        for (int i = 0; i < reqs.length; i++) {
            EmbeddedReadRequest e = reqs[i];
            if (e.getOffset() < 0 || e.BytesToRead() < 0) {
                throw new XrootdException(kXR_ArgInvalid, "negative offset or length");
            }
            OpenFile file = file(e.getFileHandle());
            ranges[i] = new Range(e.getOffset(), e.BytesToRead());
            // Clients require every element in full; the reference server fails the
            // whole request when one extends past EOF, and so do we.
            if (ranges[i].end() > file.info().size()) {
                throw new XrootdException(kXR_FSError, "vector read past end of file");
            }
            total += ranges[i].length();
            byFile.computeIfAbsent(file, f -> new ArrayList<>()).add(i);
        }
        if (total > MAX_READ_BYTES) {
            throw new XrootdException(kXR_ArgTooLong, "vector read larger than " + MAX_READ_BYTES);
        }

        ByteBuf[] data = new ByteBuf[reqs.length];
        List<CompletableFuture<Void>> parts = new ArrayList<>(byFile.size());
        byFile.forEach((file, idx) -> {
            List<Range> rs = idx.stream().map(i -> ranges[i]).toList();
            parts.add(file.readv(rs).thenAccept(bufs -> {
                for (int k = 0; k < idx.size(); k++) {
                    data[idx.get(k)] = Unpooled.wrappedBuffer(bufs.get(k));
                }
            }));
        });
        reply(msg, CompletableFuture.allOf(parts.toArray(CompletableFuture[]::new)),
                v -> new ReadVResponse(msg, reqs, data, 0, reqs.length, false));
        return null;
    }

    @Override
    protected Object doOnClose(ChannelHandlerContext ctx, CloseRequest msg) throws XrootdException {
        OpenFile file = files.remove(msg.getFileHandle());
        if (file == null) {
            throw new XrootdException(kXR_FileNotOpen, "invalid file handle");
        }
        file.close();
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
                default -> name;
            }).append('\n');
        }
        return new QueryResponse(msg, sb.toString());
    }

    @Override
    protected Object doOnPing(ChannelHandlerContext ctx, PingRequest msg) {
        return new OkResponse<>(msg);
    }

    private OpenFile file(int fh) throws XrootdException {
        OpenFile file = files.get(fh);
        if (file == null) {
            throw new XrootdException(kXR_FileNotOpen, "invalid file handle");
        }
        return file;
    }

    /** Answers {@code msg} on the event loop when {@code f} completes. */
    private <T, R extends XrootdRequest> void reply(R msg, CompletableFuture<T> f,
            Function<T, Object> response) {
        f.whenComplete((v, err) -> onLoop(() -> {
            if (err != null) {
                fail(msg, err);
            } else if (ctx.channel().isActive()) {
                respond(ctx, response.apply(v));
            }
        }));
    }

    private void onLoop(Runnable r) {
        if (ctx.executor().inEventLoop()) {
            r.run();
        } else {
            ctx.executor().execute(r);
        }
    }

    private void fail(XrootdRequest msg, Throwable err) {
        while (err instanceof CompletionException && err.getCause() != null) {
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
