package org.atlasml.ixcache.origin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connections to origins, up to {@link OriginConfig#maxConnsPerHost()} per
 * host:port, shared by all files on that origin. A new connection is opened
 * when every existing one is busy, and unused ones are closed after
 * {@link OriginConfig#idleTimeout()}.
 */
final class OriginPool implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OriginPool.class);

    private final EventLoopGroup group;
    private final OriginConfig cfg;
    private final GsiClient gsi;
    private final Map<String, List<CompletableFuture<OriginConnection>>> hosts = new HashMap<>(); // guarded by this
    private final ScheduledFuture<?> reaper;

    OriginPool(EventLoopGroup group, OriginConfig cfg, GsiClient gsi) {
        this.group = group;
        this.cfg = cfg;
        this.gsi = gsi;
        this.reaper = group.scheduleWithFixedDelay(this::closeIdle, 60, 60, TimeUnit.SECONDS);
    }

    /** A logged-in connection to {@code host:port}, existing or new. */
    synchronized CompletableFuture<OriginConnection> acquire(String host, int port) {
        List<CompletableFuture<OriginConnection>> conns =
                hosts.computeIfAbsent(host + ":" + port, k -> new ArrayList<>());
        conns.removeIf(f -> f.isCompletedExceptionally() || (f.isDone() && !f.join().isActive()));

        OriginConnection best = null;
        for (CompletableFuture<OriginConnection> f : conns) {
            if (f.isDone()) {
                OriginConnection c = f.join();
                if (best == null || c.outstanding() < best.outstanding()) {
                    best = c;
                }
            }
        }
        if (best != null && (best.outstanding() < cfg.maxStreamsPerConn() || conns.size() >= cfg.maxConnsPerHost())) {
            return CompletableFuture.completedFuture(best);
        }
        if (conns.size() < cfg.maxConnsPerHost()) {
            CompletableFuture<OriginConnection> f = OriginConnection.connect(group, host, port, cfg, gsi);
            conns.add(f);
            return f;
        }
        // Every slot is still connecting: wait for one of them.
        return conns.get(0);
    }

    private void closeIdle() {
        long limit = cfg.idleTimeout().toNanos();
        List<OriginConnection> idle = new ArrayList<>();
        synchronized (this) {
            for (List<CompletableFuture<OriginConnection>> conns : hosts.values()) {
                conns.removeIf(f -> {
                    if (f.isDone() && !f.isCompletedExceptionally() && f.join().idleNanos() > limit) {
                        idle.add(f.join());
                        return true;
                    }
                    return false;
                });
            }
            hosts.values().removeIf(List::isEmpty);
        }
        for (OriginConnection c : idle) {
            log.debug("closing idle connection to origin {}", c);
            c.close();
        }
    }

    @Override
    public synchronized void close() {
        reaper.cancel(false);
        for (List<CompletableFuture<OriginConnection>> conns : hosts.values()) {
            for (CompletableFuture<OriginConnection> f : conns) {
                f.thenAccept(OriginConnection::close);
            }
        }
        hosts.clear();
    }
}
