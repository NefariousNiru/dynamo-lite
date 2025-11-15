// file: src/main/java/io/dynlite/storage/DurableStore.java
package io.dynlite.storage;

import io.dynlite.core.VersionedValue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Durable key-value store implementation.
 * <p>
 * Responsibilities:
 *  - Maintain an in-memory map: key -> VersionedValue.
 *  - On write:
 *      1) Serialize opId+key+value to a WAL record.
 *      2) Append+fsync to WAL.
 *      3) If deduper says this opId is new, apply it to memory.
 *      4) Rotate WAL segment if needed.
 *      5) Possibly trigger a full snapshot based on SnapshotPolicy.
 * <p>
 *  - On startup:
 *      1) Load the latest snapshot (if any) into memory.
 *      2) Replay WAL records, applying each opId exactly once.
 * <p>
 * Note:
 *  - Currently we store a single VersionedValue per key.
 *    Multi-sibling support will extend this to multiple versions.
 */
public class DurableStore implements KeyValueStore {
    private final Map<String, VersionedValue> mem = new ConcurrentHashMap<>();
    private final Wal wal;
    private final Snapshotter snaps;
    private final OpIdDeduper dedupe;
    private final SnapshotPolicy snapPolicy = new SnapshotPolicy(50_000);

    public DurableStore(Wal wal, Snapshotter snaps, OpIdDeduper dedupe) {
        this.wal = wal; this.snaps = snaps; this.dedupe = dedupe;
        recover();
    }

    @Override
    public synchronized void put(String key, VersionedValue value, String opId) {
        // 1) serialize record
        var payload = RecordCodec.encode(opId, key, value);

        // 2) append + fsync to Wal. If process crashes after this call returns, recovery will still see this
        wal.append(payload);

        // 3) Apply to in-memory map once per logical operation.
        //    If we have already seen this opId, we skip to keep semantics idempotent.
        if (dedupe.firstTime(opId)) {
            applyToMemory(key, value);
        }

        // 4) maybe rotate and maybe snapshot on thresholds
        wal.rotateIfNeeded();
        snapPolicy.maybeSnapshot(mem, snaps);
    }

    @Override
    public VersionedValue get(String key) { return mem.get(key); }

    /**
     * Recovery procedure called from constructor:
     *  1) Seed memory from the latest snapshot (if present).
     *  2) Replay WAL records in order, applying each opId at most once.
     */
    private void recover() {
        // 1) load snapshot
        var loaded = snaps.loadLatest();
        if (loaded != null) mem.putAll(loaded.data());
        // 2) replay WAL
        try (var r = wal.openReader()) {
            for (byte[] payload; (payload = r.next()) != null; ) {
                var rec = RecordCodec.decode(payload);
                if (dedupe.firstTime(rec.opId())) {
                    applyToMemory(rec.key(), rec.value());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Apply a logical mutation to the in-memory map.
     * Tombstones remove keys; non-tombstones upsert.
     */
    private void applyToMemory(String key, VersionedValue val) {
        if (val.tombstone()) mem.remove(key);
        else mem.put(key, val);
    }
}
