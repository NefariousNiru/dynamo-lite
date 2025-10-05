package io.dynlite.storage;

import io.dynlite.core.VersionedValue;

/**
 * Minimal KV interface. Values are immutable envelopes with vector clocks.
 */
public interface KeyValueStore {
    /**
     * Put or delete (tombstone=true) a key. Must be durable before returning.
     */
    void put(String key, VersionedValue value, String opId);

    /**
     * Get current value or null if absent or tombstoned.
     */
    VersionedValue get(String key);
}
