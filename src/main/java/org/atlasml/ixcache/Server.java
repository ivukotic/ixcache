package org.atlasml.ixcache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.atlasml.ixcache.cache.Cache;
import org.atlasml.ixcache.cache.CacheConfig;
import org.atlasml.ixcache.cache.MultiCache;
import org.atlasml.ixcache.health.Health;
import org.atlasml.ixcache.health.HealthServer;
import org.atlasml.ixcache.health.Heartbeat;
import org.atlasml.ixcache.origin.OriginBackend;
import org.atlasml.ixcache.origin.OriginConfig;
import org.atlasml.ixcache.server.Backend;
import org.atlasml.ixcache.server.CachingBackend;
import org.atlasml.ixcache.server.LocalBackend;
import org.atlasml.ixcache.server.ServerConfig;
import org.atlasml.ixcache.server.XrootdServer;
import org.atlasml.ixcache.telemetry.Telemetry;
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

        int healthPort = Integer.parseInt(env.getOrDefault("IXCACHE_HEALTH_PORT", "8092"));
        Optional<Heartbeat.Config> heartbeatCfg = Heartbeat.Config.fromEnv(env);

        Backend backend;
        List<AutoCloseable> onExit = new ArrayList<>();
        OriginBackend origins = null;
        if (cfg.localRoot().isPresent()) {
            log.warn("serving local files from {} instead of origins", cfg.localRoot().get());
            backend = new LocalBackend(cfg.localRoot().get(), io);
        } else {
            origins = new OriginBackend(OriginConfig.fromEnv(env));
            onExit.add(origins);
            backend = origins;
        }
        MultiCache cache = null;
        String cacheDir = env.get("IXCACHE_CACHE_DIR");
        if (cacheDir != null && !cacheDir.isBlank()) {
            cache = MultiCache.open(CacheConfig.fromEnv(env));
            onExit.add(0, cache); // flush metadata before origin connections go
            for (Cache disk : cache.disks()) {
                CacheConfig cc = disk.config();
                log.info("cache at {}: {} bytes, watermarks {}/{}, block size {}, {} bytes in use",
                        cc.root(), cc.capacityBytes(), cc.lowWatermark(), cc.highWatermark(),
                        cc.blockSize(), disk.usedBytes());
            }
            log.info("cache: {} directories, {} of {} bytes in use", cache.disks().size(),
                    cache.usedBytes(), cache.capacityBytes());
            backend = new CachingBackend(cache, backend, io);
        } else {
            log.warn("IXCACHE_CACHE_DIR is not set: passing reads through without caching");
        }

        Optional<Telemetry> telemetry = Telemetry.fromEnv(env);
        if (telemetry.isEmpty()) {
            log.info("IXCACHE_TELEMETRY is not set: no transfer records are sent");
        }
        XrootdServer server = XrootdServer.start(cfg, backend, telemetry.orElse(null));
        MultiCache disks = cache;
        Optional<Heartbeat> heartbeat = heartbeatCfg.map(h ->
                new Heartbeat(h, () -> disks == null ? 0 : disks.capacityBytes()));
        HealthServer health = HealthServer.start(healthPort,
                new Health(server, cache, origins, OriginConfig.fromEnv(env).caDir()), heartbeat);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down");
            health.close();
            server.close();
            telemetry.ifPresent(Telemetry::close); // after the server: flushes records of closed connections
            for (AutoCloseable c : onExit) {
                try {
                    c.close();
                } catch (Exception e) {
                    log.warn("closing {} failed", c, e);
                }
            }
            io.close();
        }, "ixcache-shutdown"));
        Thread.currentThread().join();
    }
}
