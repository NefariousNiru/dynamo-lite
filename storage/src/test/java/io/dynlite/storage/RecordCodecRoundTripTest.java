package io.dynlite.storage;

import io.dynlite.core.VectorClock;
import io.dynlite.core.VersionedValue;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RecordCodecRoundTripTest {

    @Test
    void encode_then_decode_preserves_fields_and_crc_matches() {
        var vc = new VectorClock(Map.of("S1", 2, "S3", 1));
        var vv = new VersionedValue("Nirupom".getBytes(), false, vc, 1_728_000_000_000L);

        var opId = "op-abc-123";
        var key  = "user:1234";

        // header(11B) + payload
        byte[] bytes = RecordCodec.encode(opId, key, vv);

        // ---- read header ----
        ByteBuffer hdr = ByteBuffer.wrap(bytes, 0, 11).order(ByteOrder.LITTLE_ENDIAN);
        short magic   = hdr.getShort();
        byte  ver     = hdr.get();
        int   length  = hdr.getInt();
        int   crc     = hdr.getInt();

        assertEquals(1, ver);
        assertEquals(bytes.length - 11, length);

        byte[] payload = new byte[length];
        System.arraycopy(bytes, 11, payload, 0, length);
        assertEquals(RecordCodec.crc32(payload), crc);

        // ---- decode payload ----
        var rec = RecordCodec.decode(payload);
        assertEquals(opId, rec.opId());
        assertEquals(key, rec.key());
        assertArrayEquals(vv.value(), rec.value().value());
        assertEquals(vv.tombstone(), rec.value().tombstone());
        assertEquals(vv.lwwMillis(), rec.value().lwwMillis());
        assertEquals(vv.clock().entries(), rec.value().clock().entries());

        // Basic magic sanity (we allow test to be readable; exact value not critical)
        assertTrue(magic != 0, "magic should be non-zero");
    }
}
