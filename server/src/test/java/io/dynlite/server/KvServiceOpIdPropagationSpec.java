// file: server/src/test/java/io/dynlite/server/KvServiceOpIdPropagationSpec.java
package io.dynlite.server;

import io.dynlite.core.VersionedValue;
import io.dynlite.storage.KeyValueStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Ensure KvService.putWithOpId / deleteWithOpId propagate the caller's opId
 * instead of always generating a new UUID.
 */
class KvServiceOpIdPropagationSpec {

    private static final class CapturingStore implements KeyValueStore {
        final List<String> seenOpIds = new ArrayList<>();

        @Override
        public void put(String key, VersionedValue value, String opId) {
            seenOpIds.add(opId);
        }

        @Override
        public VersionedValue get(String key) {
            return null;
        }

        @Override
        public java.util.List<VersionedValue> getSiblings(String key) {
            return java.util.List.of();
        }
    }

    @Test
    void explicit_opId_is_used_verbatim() {
        CapturingStore store = new CapturingStore();
        KvService kv = new KvService(store, "node-a");

        String clientOpId = "client-op-123";
        kv.put("k", "dGVzdA==", "node-a", clientOpId);

        assertEquals(1, store.seenOpIds.size());
        assertEquals(clientOpId, store.seenOpIds.getFirst());
    }

    @Test
    void null_or_blank_opId_generates_new_id() {
        CapturingStore store = new CapturingStore();
        KvService kv = new KvService(store, "node-a");

        kv.put("k", "dGVzdA==", "node-a", null);
        kv.delete("k", "node-a", "   ");

        assertEquals(2, store.seenOpIds.size());
        assertNotNull(store.seenOpIds.getFirst());
        assertFalse(store.seenOpIds.getFirst().isBlank());
        assertNotNull(store.seenOpIds.get(1));
        assertFalse(store.seenOpIds.get(1).isBlank());
        assertNotEquals(store.seenOpIds.getFirst(), store.seenOpIds.get(1));
    }
}
