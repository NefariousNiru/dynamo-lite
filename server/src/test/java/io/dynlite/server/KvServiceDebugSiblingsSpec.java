// file: src/test/java/io/dynlite/server/KvServiceDebugSiblingsSpec.java
package io.dynlite.server;

import io.dynlite.core.VectorClock;
import io.dynlite.core.VersionedValue;
import io.dynlite.storage.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the debug sibling view exposed by KvService.
 *
 * Focus: shape and encoding of the sibling list, not HTTP wiring.
 */
class KvServiceDebugSiblingsSpec {

    @TempDir
    Path walDir;

    @TempDir
    Path snapDir;

    private static VersionedValue vv(String value, Map<String, Integer> vc, long ts) {
        byte[] bytes = (value == null) ? null : value.getBytes();
        return new VersionedValue(bytes, value == null, new VectorClock(vc), ts);
    }

    @Test
    void siblings_view_exposes_all_siblings_with_base64_and_clocks() {
        DurableStore store = new DurableStore(
                new FileWal(walDir, 1L << 60),
                new FileSnapshotter(snapDir),
                new TestDeduper()
        );

        // Create two concurrent siblings for "k".
        VersionedValue v1 = vv("v1", Map.of("A", 1), 1_000L);
        VersionedValue v2 = vv("v2", Map.of("B", 1), 2_000L);

        store.put("k", v1, UUID.randomUUID().toString());
        store.put("k", v2, UUID.randomUUID().toString());

        KvService svc = new KvService(store, "coord-1");

        List<KvService.Sibling> sibs = svc.siblings("k");
        assertEquals(2, sibs.size(), "Expected two siblings in debug view");

        // Check that Base64 encoding and clocks are wired correctly.
        KvService.Sibling s1 = sibs.get(0);
        KvService.Sibling s2 = sibs.get(1);

        // Just assert that both payloads decode and match either v1 or v2.
        var decoded1 = new String(Base64.getDecoder().decode(s1.valueBase64()));
        var decoded2 = new String(Base64.getDecoder().decode(s2.valueBase64()));
        assertTrue(
                (decoded1.equals("v1") && decoded2.equals("v2")) ||
                        (decoded1.equals("v2") && decoded2.equals("v1"))
        );

        // Clocks must include their respective node ids.
        assertTrue(s1.clock().containsKey("A") || s1.clock().containsKey("B"));
        assertTrue(s2.clock().containsKey("A") || s2.clock().containsKey("B"));
    }

    @Test
    void siblings_view_empty_for_unknown_key() {
        DurableStore store = new DurableStore(
                new FileWal(walDir, 1L << 60),
                new FileSnapshotter(snapDir),
                new TestDeduper()
        );

        KvService svc = new KvService(store, "coord-1");

        List<KvService.Sibling> sibs = svc.siblings("no-such-key");
        assertTrue(sibs.isEmpty());
    }
}
