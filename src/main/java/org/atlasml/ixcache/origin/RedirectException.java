package org.atlasml.ixcache.origin;

/**
 * The origin sent {@code kXR_redirect}: retry at {@code host:port}, adding
 * {@code cgi} (may be empty) to the request's opaque data.
 */
final class RedirectException extends Exception {

    private static final long serialVersionUID = 1L;

    final String host;
    final int port;
    final String cgi;

    RedirectException(String host, int port, String cgi) {
        super("redirect to " + host + ":" + port, null, false, false);
        this.host = host;
        this.port = port;
        this.cgi = cgi;
    }

    /** Parses the body of a redirect: a port and {@code host[?cgi]}. */
    static RedirectException parse(int port, String target) {
        String host = target;
        String cgi = "";
        int q = target.indexOf('?');
        if (q >= 0) {
            host = target.substring(0, q);
            cgi = target.substring(q + 1);
        }
        // A URL instead of a host name (port < 0 in newer servers).
        int scheme = host.indexOf("://");
        if (scheme >= 0) {
            String rest = host.substring(scheme + 3);
            int slash = rest.indexOf('/');
            String authority = slash < 0 ? rest : rest.substring(0, slash);
            int colon = authority.lastIndexOf(':');
            if (colon > 0 && authority.indexOf(']') < colon) {
                port = Integer.parseInt(authority.substring(colon + 1));
                authority = authority.substring(0, colon);
            }
            host = authority;
        }
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        return new RedirectException(host, port > 0 ? port : 1094, cgi);
    }
}
