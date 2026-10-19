package org.atlasml.ixcache.cache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/** Origin serving deterministic content, recording every request. */
final class FakeOrigin implements OriginReader {

    final long size;
    final List<List<Range>> requests = new CopyOnWriteArrayList<>();
    volatile CompletableFuture<Void> gate = CompletableFuture.completedFuture(null);
    volatile boolean failNext;

    FakeOrigin(long size) {
        this.size = size;
    }

    static byte byteAt(long pos) {
        return (byte) ((pos * 31) ^ (pos >>> 11));
    }

    static ByteBuffer expected(long offset, int length) {
        ByteBuffer b = ByteBuffer.allocate(length);
        for (int i = 0; i < length; i++) {
            b.put(i, byteAt(offset + i));
        }
        return b;
    }

    long bytesRequested() {
        return requests.stream().flatMap(List::stream).mapToLong(Range::length).sum();
    }

    @Override
    public CompletableFuture<List<ByteBuffer>> readv(List<Range> ranges) {
        requests.add(List.copyOf(ranges));
        if (failNext) {
            failNext = false;
            return CompletableFuture.failedFuture(new IOException("origin down"));
        }
        return gate.thenApplyAsync(v -> {
            List<ByteBuffer> out = new ArrayList<>();
            for (Range r : ranges) {
                out.add(expected(r.offset(), (int) Math.min(r.length(), size - r.offset())));
            }
            return out;
        });
    }
}
