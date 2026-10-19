package org.atlasml.ixcache.server;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.atlasml.ixcache.cache.Range;
import org.atlasml.ixcache.path.RequestPath;

/**
 * Serves files from a local directory, for testing the front end without an
 * origin. The origin part of {@code /root://origin//path} is ignored and
 * {@code path} is resolved under the root; a plain {@code /path} works too.
 */
public final class LocalBackend implements Backend {

    private final Path root;
    private final ExecutorService io;

    public LocalBackend(Path root, ExecutorService io) {
        this.root = root.toAbsolutePath().normalize();
        this.io = io;
    }

    @Override
    public CompletableFuture<OpenFile> open(String path, String opaque) {
        return async(() -> {
            Path file = resolve(path);
            try {
                FileChannel ch = FileChannel.open(file, StandardOpenOption.READ);
                return new LocalFile(ch, info(file));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public CompletableFuture<FileInfo> stat(String path, String opaque) {
        return async(() -> info(resolve(path)));
    }

    private Path resolve(String clientPath) {
        String p = clientPath.contains(":/") ? RequestPath.parse(clientPath).path() : clientPath;
        Path file = root.resolve(p.replaceFirst("^/+", "")).normalize();
        if (!file.startsWith(root)) {
            throw new IllegalArgumentException("path escapes the export root: " + clientPath);
        }
        return file;
    }

    private static FileInfo info(Path file) {
        try {
            BasicFileAttributes a = Files.readAttributes(file, BasicFileAttributes.class);
            if (!a.isRegularFile()) {
                throw new NoSuchFileException(file.toString(), null, "not a regular file");
            }
            return new FileInfo(a.size(), a.lastModifiedTime().toMillis() / 1000);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private <T> CompletableFuture<T> async(Supplier<T> s) {
        return CompletableFuture.supplyAsync(s, io).exceptionallyCompose(t ->
                CompletableFuture.failedFuture(unwrap(t)));
    }

    /** Strips the wrappers added by {@code supplyAsync} and {@link UncheckedIOException}. */
    private static Throwable unwrap(Throwable t) {
        if (t instanceof CompletionException && t.getCause() != null) {
            t = t.getCause();
        }
        if (t instanceof UncheckedIOException u) {
            return u.getCause();
        }
        return t;
    }

    private final class LocalFile implements OpenFile {

        private final FileChannel ch;
        private final FileInfo info;

        LocalFile(FileChannel ch, FileInfo info) {
            this.ch = ch;
            this.info = info;
        }

        @Override
        public FileInfo info() {
            return info;
        }

        @Override
        public CompletableFuture<List<ByteBuffer>> readv(List<Range> ranges) {
            return async(() -> {
                List<ByteBuffer> out = new ArrayList<>(ranges.size());
                try {
                    for (Range r : ranges) {
                        ByteBuffer b = ByteBuffer.allocate(r.length());
                        long pos = r.offset();
                        while (b.hasRemaining()) {
                            int n = ch.read(b, pos);
                            if (n < 0) {
                                throw new EOFException("short read at " + pos);
                            }
                            pos += n;
                        }
                        out.add(b.flip());
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return out;
            });
        }

        @Override
        public void close() {
            try {
                ch.close();
            } catch (IOException e) {
                // nothing useful to do; the channel is gone either way
            }
        }
    }
}
