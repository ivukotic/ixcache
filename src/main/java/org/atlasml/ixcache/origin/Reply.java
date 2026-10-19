package org.atlasml.ixcache.origin;

import io.netty.buffer.ByteBuf;

/**
 * A complete reply: {@code kXR_ok} (with all {@code kXR_oksofar} parts joined)
 * or {@code kXR_authmore}. Errors, redirects and waits never reach callers as
 * replies. The receiver owns {@code data} and must release it.
 */
record Reply(int status, ByteBuf data) {
}
