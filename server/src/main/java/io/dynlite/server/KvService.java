// file: src/main/java/io/dynlite/server/KvService.java
package io.dynlite.server;

import io.dynlite.core.VectorClock;
import io.dynlite.core.VersionedValue;
import io.dynlite.storage.KeyValueStore;

import java.util.Base64;
import java.util.Map;
import java.util.UUID;

/**
 * Application service for key-value operations.
 * <p>
 * Responsibilities:
 *  - Hide storage details (WAL, snapshots, etc.) from the HTTP layer.
 *  - Bump vector clocks for writes and deletes.
 *  - Generate unique opIds per logical mutation.
 *  - Encode/decode Base64 payloads expected by the API.
 * <p>
 * This is also where replication/quorum logic would go in a clustered version:
 *  - The local KeyValueStore would become "one replica".
 *  - KvService would coordinate writes to multiple replicas using a hash ring.
 */
public final class KvService {
    private final KeyValueStore store;
    private final String nodeId; // used as default coordinator

    public KvService(KeyValueStore store, String nodeId) {
        this.store = store;
        this.nodeId = nodeId;
    }

    /**
     * Put bytes for a key.
     * Steps:
     *  1) Decode Base64 payload into bytes.
     *  2) Read current value from store (if any).
     *  3) Compute base vector clock (empty or existing clock).
     *  4) Bump the clock at coordNodeId (or this.nodeId if not provided).
     *  5) Create a new VersionedValue.
     *  6) Generate a fresh opId and call store.put.
     *  7) Return view model with tombstone=false, lwwMillis, and clock entries.
     */
    public Result put(String key, String base64, String coordNodeId) {
        byte[] bytes = decode(base64);
        var current = store.get(key);
        var baseClock = current == null ? VectorClock.empty() : current.clock();
        var bumped = baseClock.bump(coordNodeId == null ? nodeId : coordNodeId);
        long now = System.currentTimeMillis();
        var vv = new VersionedValue(bytes, false, bumped, now);
        String opId = UUID.randomUUID().toString();
        store.put(key, vv, opId);
        return new Result(false, now, bumped.entries());
    }

    /**
     * Logically delete key via a tombstone.
     * Steps mirror put():
     *  - bump vector clock,
     *  - create VersionedValue with value=null, tombstone=true,
     *  - write through the store.
     */
    public Result delete(String key, String coordNodeId) {
        var current = store.get(key);
        var baseClock = current == null ? VectorClock.empty() : current.clock();
        var bumped = baseClock.bump(coordNodeId == null ? nodeId : coordNodeId);
        long now = System.currentTimeMillis();
        var vv = new VersionedValue(null, true, bumped, now);
        String opId = UUID.randomUUID().toString();
        store.put(key, vv, opId);
        return new Result(true, now, bumped.entries());
    }

    /**
     * Read the current value for a key.
     * Semantics:
     *  - If no value exists or the latest value is a tombstone, we treat the key
     *    as absent and return found=false.
     *  - Otherwise we return Base64-encoded bytes and the vector clock.
     */
    public Read get(String key) {
        var vv = store.get(key);
        if (vv == null || vv.tombstone()) return new Read(false, null, Map.of());
        String base64 = Base64.getEncoder().encodeToString(vv.value());
        return new Read(true, base64, vv.clock().entries());
    }

    // ------------ view models ------------

    /** Result of a mutation (PUT or DELETE) */
    public record Result(boolean tombstone, long lwwMillis, Map<String,Integer> clock) {}

    /** Result of a READ */
    public record Read(boolean found, String base64, Map<String,Integer> clock) {}

    // ------------ helpers ------------
    private static byte[] decode(String b64) {
        if (b64 == null || b64.isBlank()) throw new IllegalArgumentException("valueBase64 is required");
        try { return Base64.getDecoder().decode(b64); }
        catch (IllegalArgumentException e) { throw new IllegalArgumentException("valueBase64 must be Base64", e); }
    }
}
