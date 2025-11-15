// file: src/main/java/io/dynlite/storage/RecordCodec.java
package io.dynlite.storage;

import io.dynlite.core.VectorClock;
import io.dynlite.core.VersionedValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Binary framing for WAL records.
 * <p>
 * Full on-disk layout:
 * <p>
 *   [HEADER (11 bytes, little-endian)]
 *     - magic   (2B)  = 0xD17E   (helps detect garbage)
 *     - version (1B)  = 1       (for future upgrades)
 *     - length  (4B)  = payload length in bytes
 *     - crc32   (4B)  = CRC32(payload)
 * <p>
 *   [PAYLOAD (length bytes, little-endian)]
 *     - opId:      int32 len + UTF-8 bytes
 *     - key:       int32 len + UTF-8 bytes
 *     - tombstone: byte (0 or 1)
 *     - lwwMillis: int64
 *     - value:     int32 len + bytes (len == -1 => null)
 *     - vcCount:   int32 number of vector clock entries
 *         repeated vcCount times:
 *           - nodeId: int32 len + UTF-8 bytes
 *           - counter: int32
 * <p>
 * The header is validated by magic/version/length and CRC when reading.
 * The payload is decoded into a (opId, key, VersionedValue) triple.
 */
final class RecordCodec {
    static final short MAGIC = (short) 0xD17E;
    static final byte  VERSION = 1;

    /** Immutable view of a decoded record. */
    static record LogRecord(String opId, String key, VersionedValue value) {}

    /** Encode a log record into header+payload bytes ready for append. */
    static byte[] encode(String opId, String key, VersionedValue vv) {
        byte[] payload = encodePayload(opId, key, vv);
        int length = payload.length;
        ByteBuffer header = ByteBuffer.allocate(2 + 1 + 4 + 4).order(ByteOrder.LITTLE_ENDIAN);
        header.putShort(MAGIC).put(VERSION).putInt(length).putInt(crc32(payload));
        header.flip();

        byte[] out = new byte[header.remaining() + payload.length];
        header.get(out, 0, header.limit());
        System.arraycopy(payload, 0, out, header.limit(), payload.length);
        return out;
    }

    /** Decode a full payload (not including header). */
    static LogRecord decode(byte[] payload) {
        ByteBuffer b = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        String opId = readString(b);
        String key = readString(b);
        boolean tombstone = b.get() != 0;
        long lww = b.getLong();
        byte[] value = readBytes(b); // null if length == -1 below
        int entries = b.getInt();
        Map<String,Integer> vv = new LinkedHashMap<>(entries);
        for (int i = 0; i < entries; i++) {
            String nodeId = readString(b);
            int counter = b.getInt();
            vv.put(nodeId, counter);
        }
        var vclock = new VectorClock(vv);
        var vval = new VersionedValue(value, tombstone, vclock, lww);
        return new LogRecord(opId, key, vval);
    }

    // ----------------- helpers -----------------

    private static byte[] encodePayload(String opId, String key, VersionedValue vv) {
        byte[] sOp = opId.getBytes(StandardCharsets.UTF_8);
        byte[] sKey = key.getBytes(StandardCharsets.UTF_8);
        byte[] val  = vv.value(); // may be null

        int vcCount = vv.clock().entries().size();
        int size = 0;
        size += 4 + sOp.length;       // opId
        size += 4 + sKey.length;      // key
        size += 1;                    // tombstone
        size += 8;                    // lwwMillis
        size += 4 + (val == null ? 0 : val.length); // value
        size += 4;                    // vc entry count
        for (var e : vv.clock().entries().entrySet()) {
            byte[] n = e.getKey().getBytes(StandardCharsets.UTF_8);
            size += 4 + n.length;       // nodeId
            size += 4;                  // counter
        }

        ByteBuffer b = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        writeBytes(b, sOp);
        writeBytes(b, sKey);
        b.put((byte) (vv.tombstone() ? 1 : 0));
        b.putLong(vv.lwwMillis());
        writeBytes(b, val);
        b.putInt(vcCount);
        for (var e : vv.clock().entries().entrySet()) {
            writeBytes(b, e.getKey().getBytes(StandardCharsets.UTF_8));
            b.putInt(e.getValue());
        }
        return b.array();
    }

    static int crc32(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, bytes.length);
        long v = crc.getValue();
        return (int) v; // CRC32 fits in unsigned int; Java int is fine for compare
    }

    private static void writeBytes(ByteBuffer b, byte[] data) {
        if (data == null) { b.putInt(-1); return; }
        b.putInt(data.length).put(data);
    }

    private static byte[] readBytes(ByteBuffer b) {
        int len = b.getInt();
        if (len == -1) return null;
        byte[] out = new byte[len];
        b.get(out);
        return out;
    }

    private static String readString(ByteBuffer b) {
        byte[] s = readBytes(b);
        if (s == null) throw new NullPointerException("Bytes read are null");
        return new String(s, StandardCharsets.UTF_8);
    }
}
