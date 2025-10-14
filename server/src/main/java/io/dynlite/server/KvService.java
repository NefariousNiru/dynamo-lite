package io.dynlite.server;

import io.dynlite.core.VectorClock;
import io.dynlite.core.VersionedValue;
import io.dynlite.storage.KeyValueStore;

import java.util.Base64;
import java.util.Map;
import java.util.UUID;

/**
 * Application layer: bumps vector clocks, generates opIds, orchestrates durable writes.
 * Keeps domain logic separate from HTTP and JSON.
 */
public final class KvService {
    private final KeyValueStore store;
    private final String nodeId;

    public KvService(KeyValueStore store, String nodeId) {
        this.store = store;
        this.nodeId = nodeId;
    }

    /** Put bytes for key, bumping vector clock entry for 'coordNodeId' (usually this.nodeId). */
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

    /** Tombstone delete: value=null, tombstone=true, clock bumped. */
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

    /** Return null when absent or tombstoned. */
    public Read get(String key) {
        var vv = store.get(key);
        if (vv == null || vv.tombstone()) return new Read(false, null, Map.of());
        String base64 = Base64.getEncoder().encodeToString(vv.value());
        return new Read(true, base64, vv.clock().entries());
    }

    // ------------ view models ------------

    public record Result(boolean tombstone, long lwwMillis, Map<String,Integer> clock) {}
    public record Read(boolean found, String base64, Map<String,Integer> clock) {}

    // ------------ helpers ------------
    private static byte[] decode(String b64) {
        if (b64 == null || b64.isBlank()) throw new IllegalArgumentException("valueBase64 is required");
        try { return Base64.getDecoder().decode(b64); }
        catch (IllegalArgumentException e) { throw new IllegalArgumentException("valueBase64 must be Base64", e); }
    }
}
