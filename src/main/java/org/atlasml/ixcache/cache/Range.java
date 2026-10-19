package org.atlasml.ixcache.cache;

/** A byte range {@code [offset, offset + length)} within a file. */
public record Range(long offset, int length) {

    public Range {
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException("bad range " + offset + "+" + length);
        }
    }

    public long end() {
        return offset + length;
    }

    /** This range limited to a file of {@code size} bytes. */
    public Range clamp(long size) {
        if (offset >= size) {
            return new Range(offset, 0);
        }
        return new Range(offset, (int) Math.min(length, size - offset));
    }
}
