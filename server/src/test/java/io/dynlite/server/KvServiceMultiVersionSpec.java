// file: src/test/java/io/dynlite/server/KvServiceMultiVersionSpec.java
package io.dynlite.server;

import io.dynlite.core.VectorClock;
import io.dynlite.core.VersionedValue;
import io.dynlite.storage.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that KvService, when wired to a multi-version DurableStore,
 * behaves like a reconciliation coordinator:
 *
 *  - It uses all siblings as context to build a base clock,
 *  - Writes a new version that dominates prior siblings,
 *  - Collapses siblings to a single winner for that key.
 */
class KvServiceMultiVersionSpec {

    @TempDir
    Path walDir;

    @TempDir
    Path snapDir;

    private static VersionedValue vv(String value, Map<String, Integer> vc, long ts) {
        byte[] bytes = (value == null) ? null : value.getBytes();
        return new VersionedValue(bytes, value == null, new VectorClock(vc), ts);
    }

    @Test
    void put_collapses_existing_siblings_into_single_resolved_version() {
        // Underlying durable store with multi-version semantics.
        DurableStore store = new DurableStore(
                new FileWal(walDir, 1L << 60),
                new FileSnapshotter(snapDir),
                new TestDeduper()
        );

        // Manually inject two concurrent siblings for "k".
        // Clocks are incomparable: one has A:1, the other B:1.
        VersionedValue vA = vv("a", Map.of("A", 1), 1_000L);
        VersionedValue vB = vv("b", Map.of("B", 1), 2_000L);

        store.put("k", vA, UUID.randomUUID().toString());
        store.put("k", vB, UUID.randomUUID().toString());

        List<VersionedValue> before = store.getSiblings("k");
        assertEquals(2, before.size(), "Precondition: two concurrent siblings exist");

        // Now go through KvService, which should treat this as reconciliation.
        KvService svc = new KvService(store, "coord-1");
        String payload = java.util.Base64.getEncoder().encodeToString("merged".getBytes());

        KvService.Result res = svc.put("k", payload, null);

        // After PUT, siblings should collapse to a single version dominated by the new clock.
        List<VersionedValue> after = store.getSiblings("k");
        assertEquals(1, after.size(), "KvService PUT should collapse siblings to one");

        VersionedValue merged = after.get(0);
        assertArrayEquals("merged".getBytes(), merged.value());

        // The new clock should have at least:
        //  - A >= 1
        //  - B >= 1
        //  - coord-1 >= 1 (bumped by KvService)
        Map<String, Integer> clock = merged.clock().entries();
        assertTrue(clock.getOrDefault("A", 0) >= 1);
        assertTrue(clock.getOrDefault("B", 0) >= 1);
        assertTrue(clock.getOrDefault("coord-1", 0) >= 1);

        // Client-facing get() must return the merged value.
        KvService.Read read = svc.get("k");
        assertTrue(read.found());
        assertEquals("merged", new String(java.util.Base64.getDecoder().decode(read.base64())));
        assertEquals(res.clock(), read.clock());
    }
}