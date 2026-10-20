package org.atlasml.ixcache.telemetry;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends one UDP datagram (a JSON {@link TransferRecord}) per closed file.
 *
 * <p>Records go through a bounded queue to one sender thread, so telemetry never
 * blocks or fails a transfer: when the queue is full or a send fails, the record
 * is dropped and counted. The collector's address is re-resolved every minute.
 */
public final class Telemetry implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Telemetry.class);

    /** Largest datagram sent; stays below common path MTUs to avoid fragmentation. */
    public static final int MAX_DATAGRAM = 1200;
    private static final int QUEUE = 10_000;
    private static final Duration RESOLVE_EVERY = Duration.ofMinutes(1);
    private static final Duration WARN_EVERY = Duration.ofMinutes(5);

    private final String host;
    private final int port;
    private final String site;
    private final String instance;
    private final BlockingQueue<TransferRecord> queue = new ArrayBlockingQueue<>(QUEUE);
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final Thread sender;
    private volatile boolean closed;

    /**
     * Telemetry to {@code IXCACHE_TELEMETRY} ({@code host:port}), labelled with
     * {@code IXCACHE_SITE} and {@code IXCACHE_INSTANCE}; empty if not set.
     */
    public static Optional<Telemetry> fromEnv(Map<String, String> env) {
        String target = env.get("IXCACHE_TELEMETRY");
        if (target == null || target.isBlank()) {
            return Optional.empty();
        }
        target = target.trim();
        int colon = target.lastIndexOf(':');
        if (colon <= 0 || colon == target.length() - 1) {
            throw new IllegalArgumentException("IXCACHE_TELEMETRY must be host:port, not '" + target + "'");
        }
        return Optional.of(new Telemetry(target.substring(0, colon), Integer.parseInt(target.substring(colon + 1)),
                env.getOrDefault("IXCACHE_SITE", ""), env.getOrDefault("IXCACHE_INSTANCE", "")));
    }

    public Telemetry(String host, int port, String site, String instance) {
        this.host = host;
        this.port = port;
        this.site = site;
        this.instance = instance;
        this.sender = Thread.ofPlatform().name("ixcache-telemetry").daemon().start(this::run);
        log.info("telemetry to {}:{}", host, port);
    }

    /** Queues {@code r} for sending; never blocks. */
    public void record(TransferRecord r) {
        if (closed || !queue.offer(r)) {
            dropped.incrementAndGet();
        }
    }

    public long sent() {
        return sent.get();
    }

    public long dropped() {
        return dropped.get();
    }

    private void run() {
        InetSocketAddress target = null;
        Instant resolvedAt = Instant.EPOCH;
        Instant warnedAt = Instant.EPOCH;
        try (DatagramChannel ch = DatagramChannel.open()) {
            while (!closed || !queue.isEmpty()) {
                TransferRecord r = queue.poll(1, TimeUnit.SECONDS);
                if (r == null) {
                    continue;
                }
                Instant now = Instant.now();
                try {
                    if (target == null || target.isUnresolved()
                            || now.isAfter(resolvedAt.plus(RESOLVE_EVERY))) {
                        target = new InetSocketAddress(host, port);
                        resolvedAt = now;
                    }
                    if (target.isUnresolved()) {
                        throw new IOException("cannot resolve " + host);
                    }
                    ch.send(ByteBuffer.wrap(r.toJson(now, site, instance, MAX_DATAGRAM)), target);
                    sent.incrementAndGet();
                } catch (IOException | RuntimeException e) {
                    dropped.incrementAndGet();
                    if (now.isAfter(warnedAt.plus(WARN_EVERY))) {
                        warnedAt = now;
                        log.warn("telemetry to {}:{} failing ({} dropped so far): {}",
                                host, port, dropped.get(), e.toString());
                    }
                }
            }
        } catch (IOException e) {
            log.error("telemetry sender stopped", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Sends what is queued (waiting up to a few seconds), then stops. */
    @Override
    public void close() {
        closed = true;
        try {
            sender.join(Duration.ofSeconds(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("telemetry: {} records sent, {} dropped", sent.get(), dropped.get());
    }
}
