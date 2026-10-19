package org.atlasml.ixcache;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.atlasml.ixcache.origin.OriginBackend;
import org.atlasml.ixcache.origin.OriginConfig;
import org.atlasml.ixcache.server.Backend;
import org.atlasml.ixcache.server.LocalBackend;
import org.atlasml.ixcache.server.ServerConfig;
import org.atlasml.ixcache.server.XrootdServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Process entry point: configures from the environment and runs until terminated. */
public final class Server {

    private static final Logger log = LoggerFactory.getLogger(Server.class);

    private Server() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> env = System.getenv();
        ServerConfig cfg = ServerConfig.fromEnv(env);
        ExecutorService io = Executors.newVirtualThreadPerTaskExecutor();

        Backend backend;
        AutoCloseable closeBackend = () -> { };
        if (cfg.localRoot().isPresent()) {
            log.warn("serving local files from {} instead of origins", cfg.localRoot().get());
            backend = new LocalBackend(cfg.localRoot().get(), io);
        } else {
            // Pass-through to origins; the cache is not in the read path yet.
            OriginBackend origins = new OriginBackend(OriginConfig.fromEnv(env));
            backend = origins;
            closeBackend = origins;
        }

        XrootdServer server = XrootdServer.start(cfg, backend);
        AutoCloseable onExit = closeBackend;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down");
            server.close();
            try {
                onExit.close();
            } catch (Exception e) {
                log.warn("closing origin connections failed", e);
            }
            io.close();
        }, "ixcache-shutdown"));
        Thread.currentThread().join();
    }
}
