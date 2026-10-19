package org.atlasml.ixcache.server;

import java.util.concurrent.CompletableFuture;

/**
 * Where the front end gets file data: the origin, the cache, or (for testing) a
 * local directory. Paths are as the client sent them, e.g.
 * {@code /root://origin//path}; {@code opaque} is the CGI part without the {@code ?}.
 *
 * <p>Futures fail with {@link java.nio.file.NoSuchFileException} for missing
 * files, {@link IllegalArgumentException} for malformed paths, and
 * {@link org.dcache.xrootd.core.XrootdException} when a specific XRootD error
 * should reach the client.
 */
public interface Backend {

    CompletableFuture<OpenFile> open(String path, String opaque);

    CompletableFuture<FileInfo> stat(String path, String opaque);
}
