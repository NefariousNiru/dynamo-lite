// file: src/main/java/io/dynlite/server/KvService.java
package io.dynlite.server;

import io.dynlite.core.VectorClock;
import io.dynlite.core.VersionedValue;
import io.dynlite.storage.KeyValueStore;

import java.util.*;

/**
 * Application service for key-value operations.
 * <p>
 * Responsibilities:
 *  - Hide storage details (WAL, snapshots, etc.) from the HTTP layer.
 *  - Bump vector clocks for writes and deletes.
 *  - Generate unique opIds per logical mutation.
 *  - Encode/decode Base64 payloads expected by the API.
 * <p>
 * Interpretation with multi-version store:
 *  - For each PUT/DELETE, we:
 *      1) Read the full sibling set for the key.
 *      2) Compute a "base clock" as the elementwise maximum across sibling clocks.
 *      3) Bump that clock at the coordinator node id.
 *      4) Write a new VersionedValue with this clock.
 *  - This treats every write as a reconciliation step that acknowledges all
 *    known histories and therefore dominates previous siblings.
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
     *  2) Fetch all known siblings for this key from the store.
     *  3) Compute base vector clock as the max over sibling clocks.
     *  4) Bump the clock at coordNodeId (or this.nodeId if not provided).
     *  5) Create a new VersionedValue (tombstone=false).
     *  6) Generate a fresh opId and call store.put.
     *  7) Return view model with tombstone=false, lwwMillis, and clock entries.
     */
    public Result put(String key, String base64, String coordNodeId) {
        byte[] bytes = decode(base64);

        VectorClock baseClock = baseClockFromSiblings(key);
        String bumpId = (coordNodeId == null || coordNodeId.isBlank())
                ? nodeId
                : coordNodeId;

        VectorClock bumped = baseClock.bump(bumpId);
        long now = System.currentTimeMillis();

        VersionedValue vv = new VersionedValue(bytes, false, bumped, now);
        String opId = UUID.randomUUID().toString();

        store.put(key, vv, opId);

        return new Result(false, now, bumped.entries());
    }

    /**
     * Logically delete key via a tombstone.
     * Steps mirror put():
     *  1) Fetch siblings and compute base clock.
     *  2) Bump clock at coordNodeId.
     *  3) Create VersionedValue with value=null, tombstone=true.
     *  4) Write through store.
     */
    public Result delete(String key, String coordNodeId) {
        VectorClock baseClock = baseClockFromSiblings(key);
        String bumpId = (coordNodeId == null || coordNodeId.isBlank())
                ? nodeId
                : coordNodeId;

        VectorClock bumped = baseClock.bump(bumpId);
        long now = System.currentTimeMillis();

        VersionedValue vv = new VersionedValue(null, true, bumped, now);
        String opId = UUID.randomUUID().toString();

        store.put(key, vv, opId);

        return new Result(true, now, bumped.entries());
    }


    /**
     * Read the current value for a key.
     * Semantics:
     *  - Delegates to store.get(key), which returns a single resolved VersionedValue
     *    (or null if no live value).
     *  - If no value exists (or only tombstones), we return found=false.
     *  - Otherwise we return Base64-encoded bytes and the vector clock.
     */
    public Read get(String key) {
        VersionedValue vv = store.get(key);
        if (vv == null || vv.tombstone()) {
            return new Read(false, null, Map.of());
        }
        String base64 = Base64.getEncoder().encodeToString(vv.value());
        return new Read(true, base64, vv.clock().entries());
    }

    /**
     * Debug/diagnostic view: return all current siblings for a key.
     * This exposes the *full* causal frontier (maximal versions) for the key,
     * not just the single resolved value used by the normal GET path.
     */
    public List<Sibling> siblings(String key) {
        List<VersionedValue> siblings = store.getSiblings(key);
        if (siblings.isEmpty()) {
            return List.of();
        }
        return siblings.stream()
                .map(v -> new Sibling(
                        v.tombstone(),
                        v.lwwMillis(),
                        v.value() == null
                                ? null
                                : Base64.getEncoder().encodeToString(v.value()),
                        v.clock().entries()
                ))
                .toList();
    }

    // ------------ view models ------------

    /** Result of a mutation (PUT or DELETE) */
    public record Result(boolean tombstone, long lwwMillis, Map<String,Integer> clock) {}

    /** Result of a READ */
    public record Read(boolean found, String base64, Map<String,Integer> clock) {}

    /**
     * Debug representation of a single sibling version.
     */
    public record Sibling(
            boolean tombstone,
            long lwwMillis,
            String valueBase64,
            Map<String, Integer> clock
    ) {}

    // ------------ helpers ------------
    /**
     * Compute a base vector clock for 'key' by taking the elementwise maximum
     * across all known siblings' clocks.
     * If the key has never been written, this returns VectorClock.empty()
     * This is equivalent to what clients would do if they merged sibling clocks
     * and sent back a new context with their write.
     */
    private VectorClock baseClockFromSiblings(String key) {
        List<VersionedValue> siblings = store.getSiblings(key);
        if (siblings.isEmpty()) {
            return VectorClock.empty();
        }
        Map<String, Integer> merged = new HashMap<>();
        for (VersionedValue v : siblings) {
            for (Map.Entry<String, Integer> e : v.clock().entries().entrySet()) {
                String id = e.getKey();
                int cnt = e.getValue();
                merged.merge(id, cnt, Math::max);
            }
        }
        return new VectorClock(merged);
    }

    private static byte[] decode(String b64) {
        if (b64 == null || b64.isBlank()) throw new IllegalArgumentException("valueBase64 is required");
        try { return Base64.getDecoder().decode(b64); }
        catch (IllegalArgumentException e) { throw new IllegalArgumentException("valueBase64 must be Base64", e); }
    }
}
