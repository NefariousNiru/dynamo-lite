package io.dynlite.storage;

import io.dynlite.core.VectorClock;
import io.dynlite.core.VersionedValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static java.nio.file.StandardOpenOption.APPEND;
import static org.junit.jupiter.api.Assertions.*;

class FileWalTornTailTest {

    @TempDir Path walDir;
    @TempDir Path snapDir;

    private static VersionedValue vv(String s, Map<String,Integer> vc, long ts) {
        return new VersionedValue(s == null ? null : s.getBytes(), s == null, new VectorClock(vc), ts);
    }

    @Test
    void replay_ignores_truncated_tail_and_applies_all_prior_records() throws Exception {
        // Prepare WAL and write two full records
        var wal = new FileWal(walDir, 1L << 60); // huge rotate threshold so single segment
        byte[] r1 = RecordCodec.encode("op1", "k1", new VersionedValue("v1".getBytes(), false, new VectorClock(Map.of("S1",1)), 1000));
        byte[] r2 = RecordCodec.encode("op2", "k2", new VersionedValue("v2".getBytes(), false, new VectorClock(Map.of("S1",2)), 2000));
        wal.append(r1);
        wal.append(r2);
        // Create a third record but append only a partial payload (simulate torn write)
        byte[] r3 = RecordCodec.encode("op3", "k3", new VersionedValue("v3".getBytes(), false, new VectorClock(Map.of("S1",3)), 3000));
        Path seg = walDir.resolve("00000001.log");
        try (OutputStream out = Files.newOutputStream(seg, APPEND)) {
            int partialLen = r3.length - 5; // cut the tail so header says "len" but payload is short
            out.write(r3, 0, partialLen);
            out.flush(); // simulate crash right here
        }
        // Close writer channel
        wal.close();

        // Recover via DurableStore constructor
        var store = new DurableStore(new FileWal(walDir, 1L << 60), new FileSnapshotter(snapDir), new TestDeduper());

        // Records 1 and 2 are applied; 3 is ignored due to truncation
        assertArrayEquals("v1".getBytes(), store.get("k1").value());
        assertArrayEquals("v2".getBytes(), store.get("k2").value());
        assertNull(store.get("k3")); // truncated tail not applied
    }
}
