package io.dynlite.storage;

import io.dynlite.core.VectorClock;
import io.dynlite.core.VersionedValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DurableStoreDurabilityTest {

    @TempDir Path walDir;
    @TempDir Path snapDir;

    private static VersionedValue vv(String s, Map<String,Integer> vc, long ts) {
        return new VersionedValue(s.getBytes(), false, new VectorClock(vc), ts);
    }

    @Test
    void latest_value_survives_restart() {
        var store1 = new DurableStore(new FileWal(walDir, 1L << 60), new FileSnapshotter(snapDir), new TestDeduper());

        store1.put("k", vv("v1", Map.of("S1",1), 1000), UUID.randomUUID().toString());
        store1.put("k", vv("v2", Map.of("S1",2), 2000), UUID.randomUUID().toString());

        // "Crash": drop reference; new instance recovers from disk
        var store2 = new DurableStore(new FileWal(walDir, 1L << 60), new FileSnapshotter(snapDir), new TestDeduper());

        assertNotNull(store2.get("k"));
        assertArrayEquals("v2".getBytes(), store2.get("k").value());
    }
}
