package org.atlasml.ixcache.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.BitSet;
import java.util.zip.CRC32;

/**
 * Persistent description of one cached file.
 *
 * <p>Layout (big-endian): magic, version, blockSize, fileSize, lastAccess,
 * key (UTF), originUrl (UTF), bitmap length in longs, bitmap longs, CRC32 of
 * everything before it.
 */
record FileMeta(String key, String originUrl, long fileSize, int blockSize, long lastAccess, BitSet present) {

    static final int MAGIC = 0x4958434D; // "IXCM"
    static final int VERSION = 1;

    int numBlocks() {
        return numBlocks(fileSize, blockSize);
    }

    static int numBlocks(long fileSize, int blockSize) {
        long n = (fileSize + blockSize - 1) / blockSize;
        if (n > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("file too large for block size: " + fileSize);
        }
        return (int) n;
    }

    /** Bytes on disk represented by the present blocks. */
    long cachedBytes() {
        long bytes = (long) present.cardinality() * blockSize;
        int last = numBlocks() - 1;
        if (last >= 0 && present.get(last)) {
            bytes -= (long) last * blockSize + blockSize - fileSize;
        }
        return bytes;
    }

    byte[] encode() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeInt(blockSize);
            out.writeLong(fileSize);
            out.writeLong(lastAccess);
            out.writeUTF(key);
            out.writeUTF(originUrl);
            long[] words = present.toLongArray();
            out.writeInt(words.length);
            for (long w : words) {
                out.writeLong(w);
            }
            CRC32 crc = new CRC32();
            crc.update(bytes.toByteArray());
            out.writeLong(crc.getValue());
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static FileMeta decode(byte[] data) throws IOException {
        if (data.length < 8) {
            throw new IOException("metadata too short");
        }
        CRC32 crc = new CRC32();
        crc.update(data, 0, data.length - 8);
        long stored = ByteBuffer.wrap(data, data.length - 8, 8).getLong();
        if (stored != crc.getValue()) {
            throw new IOException("metadata checksum mismatch");
        }
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(data, 0, data.length - 8));
        if (in.readInt() != MAGIC) {
            throw new IOException("bad metadata magic");
        }
        int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("unsupported metadata version " + version);
        }
        int blockSize = in.readInt();
        long fileSize = in.readLong();
        long lastAccess = in.readLong();
        String key = in.readUTF();
        String originUrl = in.readUTF();
        int n = in.readInt();
        if (n < 0 || n > (numBlocks(fileSize, blockSize) + 63) / 64) {
            throw new IOException("bad bitmap length " + n);
        }
        long[] words = new long[n];
        for (int i = 0; i < n; i++) {
            words[i] = in.readLong();
        }
        return new FileMeta(key, originUrl, fileSize, blockSize, lastAccess, BitSet.valueOf(words));
    }

    /** Writes atomically: temp file, fsync, rename. */
    void write(Path target) throws IOException {
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buf = ByteBuffer.wrap(encode());
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            ch.force(true);
        }
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    static FileMeta read(Path path) throws IOException {
        return decode(Files.readAllBytes(path));
    }
}
