package org.atlasml.ixcache.server;

import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_NotAuthorized;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_endsess;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_login;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_protocol;

import io.netty.channel.ChannelHandlerContext;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.core.XrootdRequestHandler;
import org.dcache.xrootd.core.XrootdSessionIdentifier;
import org.dcache.xrootd.protocol.messages.LoginRequest;
import org.dcache.xrootd.protocol.messages.LoginResponse;
import org.dcache.xrootd.protocol.messages.OkResponse;
import org.dcache.xrootd.protocol.messages.XrootdRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Login without authentication, as native XCache does: the login reply lists no
 * security protocols, so clients go straight to file requests.
 *
 * <p>xrootd4j's own "none" mode is different: it offers the {@code unix}
 * protocol, which fails for clients restricted to other protocols
 * (e.g. {@code XrdSecPROTOCOL=gsi}).
 */
final class AnonymousLoginHandler extends XrootdRequestHandler {

    private static final Logger log = LoggerFactory.getLogger(AnonymousLoginHandler.class);

    private boolean loggedIn;

    @Override
    protected Object getResponse(ChannelHandlerContext ctx, XrootdRequest req) throws Exception {
        switch (req.getRequestId()) {
            case kXR_protocol:
                ctx.fireChannelRead(req);
                return null;
            case kXR_login:
                LoginRequest login = (LoginRequest) req;
                loggedIn = true;
                log.debug("{} logged in as {} (no authentication)",
                        ctx.channel().remoteAddress(), login.getUserName());
                return new LoginResponse(login, new XrootdSessionIdentifier(), "");
            case kXR_endsess:
                // Clients end their previous session after reconnecting; sessions
                // hold no state here, so there is nothing to end.
                return new OkResponse<>(req);
            default:
                if (!loggedIn) {
                    throw new XrootdException(kXR_NotAuthorized, "Login required");
                }
                ctx.fireChannelRead(req);
                return null;
        }
    }
}
