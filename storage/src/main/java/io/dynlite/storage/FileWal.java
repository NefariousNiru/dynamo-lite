package io.dynlite.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.*;

/**
 * File-backed WAL with simple framing and CRC.
 * Append: write header+payload, then force() to fsync.
 * Reader: sequentially scans, stops at first truncated or bad-CRC record.
 */
public class FileWal implements Wal {
    private final Path dir;
    private final long rotateBytes;
    private FileChannel ch;
    private Path current;
    private long writtenInSegment = 0;

    public FileWal(Path dir, long rotateBytes) {
        this.dir = dir;
        this.rotateBytes = rotateBytes;
        try { Files.createDirectories(dir); } catch (IOException e) { throw new RuntimeException(e); }
        openNewestOrCreate();
    }

    @Override
    public void append(byte[] serializedRecord) {
        try {
            ch.write(ByteBuffer.wrap(serializedRecord));
            ch.force(true); // fsync: metadata too, so new file appears durable after rotation
            writtenInSegment += serializedRecord.length;
        } catch (IOException e) {
            throw new RuntimeException("WAL append failed", e);
        }
    }

    @Override
    public void rotateIfNeeded() {
        if (writtenInSegment < rotateBytes) return;
        try {
            ch.close();
            String next = String.format("%08d.log", Integer.parseInt(
                    current.getFileName().toString().replace(".log","")) + 1);
            current = dir.resolve(next);
            ch = FileChannel.open(current, CREATE, WRITE, READ);
            writtenInSegment = 0;
        } catch (IOException e) { throw new RuntimeException(e); }
    }

    @Override
    public WalReader openReader() { return new Reader(dir); }

    @Override
    public void close() throws Exception { if (ch != null) ch.close(); }

    private void openNewestOrCreate() {
        try {
            var seg = Files.list(dir)
                    .filter(p -> p.getFileName().toString().endsWith(".log"))
                    .sorted()
                    .reduce((a,b) -> b) // last
                    .orElse(dir.resolve("00000001.log"));
            boolean exists = Files.exists(seg);
            current = seg;
            ch = FileChannel.open(current, CREATE, WRITE, READ);
            writtenInSegment = ch.size();
            ch.position(writtenInSegment);
        } catch (IOException e) { throw new RuntimeException(e); }
    }

    private static final class Reader implements WalReader {
        private final FileChannel ch;
        private long pos = 0;

        Reader(Path dir) {
            try {
                var seg = Files.list(dir)
                        .filter(p -> p.getFileName().toString().endsWith(".log"))
                        .sorted()
                        .findFirst()
                        .orElse(dir.resolve("00000001.log"));
                ch = FileChannel.open(seg, READ);
            } catch (IOException e) { throw new RuntimeException(e); }
        }

        @Override
        public byte[] next() {
            try {
                ByteBuffer hdr = ByteBuffer.allocate(11).order(ByteOrder.LITTLE_ENDIAN);
                int read = ch.read(hdr, pos);
                if (read == -1 || read == 0) return null; // EOF or empty
                if (read < 11) return null; // truncated header at tail, stop
                hdr.flip();
                short magic = hdr.getShort();
                byte ver = hdr.get();
                int len = hdr.getInt();
                int crc = hdr.getInt();
                if (magic != RecordCodec.MAGIC || ver != RecordCodec.VERSION || len < 0) return null;
                ByteBuffer payload = ByteBuffer.allocate(len);
                int r2 = ch.read(payload, pos + 11);
                if (r2 < len) return null; // truncated payload, stop
                byte[] bytes = payload.array();
                if (RecordCodec.crc32(bytes) != crc) return null; // bad tail, stop
                pos += 11L + len;
                return bytes;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override public void close() throws Exception { ch.close(); }
    }
}
