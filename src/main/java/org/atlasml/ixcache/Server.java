package org.atlasml.ixcache;

import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_Unsupported;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.atlasml.ixcache.server.Backend;
import org.atlasml.ixcache.server.FileInfo;
import org.atlasml.ixcache.server.LocalBackend;
import org.atlasml.ixcache.server.OpenFile;
import org.atlasml.ixcache.server.ServerConfig;
import org.atlasml.ixcache.server.XrootdServer;
import org.dcache.xrootd.core.XrootdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Process entry point: configures from the environment and runs until terminated. */
public final class Server {

    private static final Logger log = LoggerFactory.getLogger(Server.class);

    private Server() {
    }

    public static void main(String[] args) throws Exception {
        ServerConfig cfg = ServerConfig.fromEnv(System.getenv());
        ExecutorService io = Executors.newVirtualThreadPerTaskExecutor();

        Backend backend;
        if (cfg.localRoot().isPresent()) {
            log.warn("serving local files from {} instead of origins", cfg.localRoot().get());
            backend = new LocalBackend(cfg.localRoot().get(), io);
        } else {
            backend = new NoOrigin();
        }

        XrootdServer server = XrootdServer.start(cfg, backend);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down");
            server.close();
            io.close();
        }, "ixcache-shutdown"));
        Thread.currentThread().join();
    }

    /** Placeholder until the origin client exists: every open and stat fails. */
    private static final class NoOrigin implements Backend {

        @Override
        public CompletableFuture<OpenFile> open(String path, String opaque) {
            return notYet();
        }

        @Override
        public CompletableFuture<FileInfo> stat(String path, String opaque) {
            return notYet();
        }

        private static <T> CompletableFuture<T> notYet() {
            return CompletableFuture.failedFuture(
                    new XrootdException(kXR_Unsupported, "origin access is not implemented yet"));
        }
    }
}
