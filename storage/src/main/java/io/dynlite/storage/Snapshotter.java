package io.dynlite.storage;

import io.dynlite.core.VersionedValue;

import java.util.Map;

/**
 * Creates and loads point-in-time snapshots to bound recovery time.
 */
public interface Snapshotter {
    /** Persist a full copy of the current map. Return snapshot id or path. */
    String writeSnapshot(Map<String, VersionedValue> current);

    /** Load the latest snapshot if present. Returns a map to seed memory. */
    LoadedSnapshot loadLatest();

    record LoadedSnapshot(String id, Map<String, VersionedValue> data) {}
}
