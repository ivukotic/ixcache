package org.atlasml.ixcache.cache;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Caps the bytes of fetched data waiting to be written to one disk. Each waiting
 * write keeps its data in memory, so a disk that falls behind the origin would
 * otherwise grow memory without bound; past the cap, data reaches the readers
 * that asked for it but is not cached.
 */
final class WriteBudget {

    private final long max;
    private final AtomicLong used = new AtomicLong();
    private final AtomicLong skipped = new AtomicLong();

    WriteBudget(long max) {
        this.max = max;
    }

    /** Reserves {@code n} bytes; false if that would exceed the cap. A write is always allowed when none waits. */
    boolean tryAcquire(long n) {
        while (true) {
            long u = used.get();
            if (u > 0 && u + n > max) {
                skipped.addAndGet(n);
                return false;
            }
            if (used.compareAndSet(u, u + n)) {
                return true;
            }
        }
    }

    void release(long n) {
        used.addAndGet(-n);
    }

    /** Bytes refused since the last call. */
    long takeSkipped() {
        return skipped.getAndSet(0);
    }
}
