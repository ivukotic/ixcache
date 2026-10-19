package org.atlasml.ixcache.origin;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.dcache.xrootd.protocol.XrootdProtocol.PROTOCOL_VERSION;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_close;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_login;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_open;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_open_read;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_protocol;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_read;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_readv;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_retstat;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_stat;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ver005;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.atlasml.ixcache.cache.Range;
import org.dcache.xrootd.tpc.protocol.messages.AbstractXrootdOutboundRequest;

/**
 * Encodes client requests. Each request is a 24-byte header (stream ID, request
 * ID, 16 parameter bytes, data length) followed by its data; the stream ID is
 * left 0 and set by {@link OriginConnection} when the request is sent.
 */
final class Requests {

    /** Largest readv element servers accept (XRootD's readv_ior_max). */
    static final int READV_IOR_MAX = 2097136;
    /** Most elements in one readv (XRootD's readv_iov_max). */
    static final int READV_IOV_MAX = 1024;

    private Requests() {
    }

    static ByteBuf protocol() {
        ByteBuf b = header(kXR_protocol);
        b.writeInt(PROTOCOL_VERSION);
        b.writeByte(0); // flags: no TLS, no security requirements wanted
        b.writeByte(0); // expect
        b.writeZero(10);
        return withData(b, null);
    }

    static ByteBuf login(String user, int pid) {
        ByteBuf b = header(kXR_login);
        b.writeInt(pid);
        byte[] u = user.getBytes(US_ASCII);
        b.writeBytes(u, 0, Math.min(u.length, 8));
        b.writeZero(8 - Math.min(u.length, 8));
        b.writeByte(0); // ability2
        b.writeByte(0); // ability
        b.writeByte(kXR_ver005); // capver: no async (kXR_attn) messages wanted
        b.writeByte(0);
        return withData(b, "xrd.appname=ixcache".getBytes(US_ASCII));
    }

    /** Opens {@code pathAndCgi} read-only, asking for its stat information. */
    static ByteBuf open(String pathAndCgi) {
        ByteBuf b = header(kXR_open);
        b.writeShort(0); // mode
        b.writeShort(kXR_open_read | kXR_retstat);
        b.writeShort(0); // optiont
        b.writeZero(6);
        b.writeZero(4); // fhtemplt
        return withData(b, pathAndCgi.getBytes(US_ASCII));
    }

    static ByteBuf stat(String pathAndCgi) {
        ByteBuf b = header(kXR_stat);
        b.writeByte(0); // options
        b.writeZero(7);
        b.writeZero(4); // optiont
        b.writeZero(4); // fhandle
        return withData(b, pathAndCgi.getBytes(US_ASCII));
    }

    static ByteBuf read(int fh, long offset, int length) {
        ByteBuf b = header(kXR_read);
        b.writeInt(fh);
        b.writeLong(offset);
        b.writeInt(length);
        return withData(b, null);
    }

    static ByteBuf readv(int fh, List<Range> elements) {
        ByteBuf b = header(kXR_readv);
        b.writeZero(15);
        b.writeByte(0); // pathid
        b.writeInt(16 * elements.size());
        for (Range r : elements) {
            b.writeInt(fh);
            b.writeInt(r.length());
            b.writeLong(r.offset());
        }
        return b;
    }

    static ByteBuf close(int fh) {
        ByteBuf b = header(kXR_close);
        b.writeInt(fh);
        b.writeZero(12);
        return withData(b, null);
    }

    /** Renders an xrootd4j request (used for GSI) into a buffer. */
    static ByteBuf render(AbstractXrootdOutboundRequest request) {
        EmbeddedChannel ch = new EmbeddedChannel(new ChannelOutboundHandlerAdapter());
        try {
            ChannelHandlerContext ctx = ch.pipeline().firstContext();
            request.writeTo(ctx, ch.newPromise());
            ch.flushOutbound();
            ByteBuf out = ch.readOutbound();
            out.setShort(0, 0);
            return out;
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    private static ByteBuf header(int requestId) {
        ByteBuf b = Unpooled.buffer(24);
        b.writeShort(0);
        b.writeShort(requestId);
        return b;
    }

    private static ByteBuf withData(ByteBuf b, byte[] data) {
        b.writeInt(data == null ? 0 : data.length);
        if (data != null) {
            b.writeBytes(data);
        }
        return b;
    }
}
