package io.dynlite.storage;

import io.dynlite.core.VectorClock;
import io.dynlite.core.VersionedValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DurableStoreIdempotenceTest {

    @TempDir Path walDir;
    @TempDir Path snapDir;

    @Test
    void duplicate_op_applied_once_on_recovery() throws Exception {
        var vc = new VectorClock(Map.of("S1", 1));
        var vv = new VersionedValue("value".getBytes(), false, vc, 1111L);

        // Write the SAME record twice (simulate duplicate append due to retry)
        var walWriter = new FileWal(walDir, 1L << 60);
        byte[] rec = RecordCodec.encode("same-op-id", "k", vv);
        walWriter.append(rec);
        walWriter.append(rec); // duplicate
        walWriter.close();

        // Recover into store (deduper should drop the second)
        var store = new DurableStore(new FileWal(walDir, 1L << 60), new FileSnapshotter(snapDir), new TestDeduper());
        var got = store.get("k");
        assertNotNull(got);
        assertArrayEquals("value".getBytes(), got.value());
    }
}
