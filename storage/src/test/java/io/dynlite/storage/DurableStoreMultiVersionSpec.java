// file: src/test/java/io/dynlite/storage/DurableStoreMultiVersionSpec.java
package io.dynlite.storage;

import io.dynlite.core.VectorClock;
import io.dynlite.core.VersionedValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DurableStoreMultiVersionSpec {

    @TempDir
    Path walDir;

    @TempDir
    Path snapDir;

    private static VersionedValue vv(String value, Map<String, Integer> vc, long ts) {
        byte[] bytes = (value == null) ? null : value.getBytes();
        return new VersionedValue(bytes, value == null, new VectorClock(vc), ts);
    }

    @Test
    void dominated_update_collapses_to_single_winner() {
        var store = new DurableStore(
                new FileWal(walDir, 1L << 60),
                new FileSnapshotter(snapDir),
                new TestDeduper()
        );

        var v1 = vv("v1", Map.of("A", 1), 1000L);
        var v2 = vv("v2", Map.of("A", 2), 2000L); // dominates v1

        store.put("k", v1, UUID.randomUUID().toString());
        store.put("k", v2, UUID.randomUUID().toString());

        List<VersionedValue> siblings = store.getSiblings("k");
        assertEquals(1, siblings.size(), "Dominance should collapse siblings to one");
        assertArrayEquals("v2".getBytes(), siblings.get(0).value());

        VersionedValue resolved = store.get("k");
        assertNotNull(resolved);
        assertArrayEquals("v2".getBytes(), resolved.value());
    }

    @Test
    void concurrent_updates_produce_siblings() {
        var store = new DurableStore(
                new FileWal(walDir, 1L << 60),
                new FileSnapshotter(snapDir),
                new TestDeduper()
        );

        // Independent clocks => concurrent.
        var vA = vv("a", Map.of("A", 1), 1000L);
        var vB = vv("b", Map.of("B", 1), 2000L);

        store.put("k", vA, "op-a");
        store.put("k", vB, "op-b");

        List<VersionedValue> siblings = store.getSiblings("k");
        assertEquals(2, siblings.size(), "Concurrent updates should yield siblings");

        // Resolved view uses LWW: b has later lwwMillis.
        VersionedValue resolved = store.get("k");
        assertNotNull(resolved);
        assertArrayEquals("b".getBytes(), resolved.value());
    }

    @Test
    void tombstone_dominates_and_hides_key_from_get() {
        var store = new DurableStore(
                new FileWal(walDir, 1L << 60),
                new FileSnapshotter(snapDir),
                new TestDeduper()
        );

        var live = vv("live", Map.of("A", 1), 1000L);
        var tomb = vv(null, Map.of("A", 2), 1100L); // same node, higher clock => dominates

        store.put("k", live, "op-live");
        store.put("k", tomb, "op-del");

        List<VersionedValue> siblings = store.getSiblings("k");
        assertEquals(1, siblings.size(), "Dominant tombstone should collapse siblings");
        assertTrue(siblings.get(0).tombstone());

        // Client-facing get() should treat as not found.
        assertNull(store.get("k"));
    }
}
