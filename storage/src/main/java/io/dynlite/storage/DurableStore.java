// file: src/main/java/io/dynlite/storage/DurableStore.java
package io.dynlite.storage;

import io.dynlite.core.ConflictResolver;
import io.dynlite.core.DefaultVersionMerger;
import io.dynlite.core.VersionMerger;
import io.dynlite.core.VersionedValue;

import java.util.ArrayList;
import java.util.List;
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
 * Multi-version semantics:
 *  - For each key we keep a list of maximal versions (siblings) under the vector
 *    clock partial order.
 *  - New writes are merged against existing siblings using VersionMerger.
 *  - Reads return a single winner chosen by ConflictResolver (LWW by lwwMillis).
 */
public class DurableStore implements KeyValueStore {
    private final Map<String, List<VersionedValue>> mem = new ConcurrentHashMap<>();
    private final Wal wal;
    private final Snapshotter snaps;
    private final OpIdDeduper dedupe;
    private final SnapshotPolicy snapPolicy = new SnapshotPolicy(50_000);

    // Merge policy: compute maximal versions under vector-clock partial order.
    private final VersionMerger versionMerger = new DefaultVersionMerger();

    // Read policy: choose a single VersionedValue from siblings.
    private final ConflictResolver resolver = new ConflictResolver.LastWriterWins();

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
    public VersionedValue get(String key) {
        List<VersionedValue> siblings = mem.get(key);
        if (siblings == null || siblings.isEmpty()) {
            return null;
        }

        // Check if there is at least one non-tombstone version.
        boolean hasLive = siblings.stream().anyMatch(v -> !v.tombstone());
        if (!hasLive) {
            // All siblings are tombstones: treat as "deleted" from client perspective.
            return null;
        }

        if (siblings.size() == 1) {
            VersionedValue only = siblings.getFirst();
            return only.tombstone() ? null : only;
        }

        // Multiple siblings (concurrent versions). Use policy to choose a winner.
        VersionedValue chosen = resolver.choose(siblings);
        return chosen.tombstone() ? null : chosen;
    }

    /**
     * Return a snapshot of the current sibling set for a key.
     */
    @Override
    public List<VersionedValue> getSiblings(String key) {
        List<VersionedValue> siblings = mem.get(key);
        if (siblings == null || siblings.isEmpty()) {
            return List.of();
        }
        return List.copyOf(siblings);
    }

    /**
     * Recovery procedure called from constructor:
     *  1) Seed memory from the latest snapshot (if present).
     *  2) Replay WAL records in order, applying each opId at most once.
     */
    private void recover() {
        // 1) load snapshot
       Snapshotter.LoadedSnapshot loaded = snaps.loadLatest();
        if (loaded != null && loaded.data() != null) {
            mem.putAll(loaded.data());
        }

        // 2) replay WAL
        try (Wal.WalReader r = wal.openReader()) {
            for (byte[] payload; (payload = r.next()) != null; ) {
                RecordCodec.LogRecord rec = RecordCodec.decode(payload);
                if (dedupe.firstTime(rec.opId())) {
                    applyToMemory(rec.key(), rec.value());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Recovery Failed", e);
        }
    }

    /**
     * Apply a single VersionedValue to the in-memory sibling set for 'key',
     * using VersionMerger to maintain only the maximal versions.
     */
    private void applyToMemory(String key, VersionedValue newVal) {
        List<VersionedValue> existing = mem.get(key);

        List<VersionedValue> candidates;
        if (existing == null || existing.isEmpty()) {
            // First time we see this key during this run.
            candidates = List.of(newVal);
        } else {
            candidates = new ArrayList<>(existing.size() + 1);
            candidates.addAll(existing);
            candidates.add(newVal);
        }

        VersionMerger.MergeResult result = versionMerger.merge(candidates);

        List<VersionedValue> newSiblings;
        if (result instanceof VersionMerger.MergeResult.Winner(VersionedValue value)) {
            newSiblings = List.of(value);
        } else if (result instanceof VersionMerger.MergeResult.Siblings(List<VersionedValue> values)) {
            newSiblings = List.copyOf(values);
        } else {
            throw new IllegalStateException("Unknown merge result type: " + result);
        }

        // We keep tombstone-only sibling sets (for replication / anti-entropy).
        // Client-facing get() will treat these as "deleted".
        mem.put(key, newSiblings);
    }

    // file: storage/src/main/java/io/dynlite/storage/DurableStore.java
    /**
     * Return a shallow snapshot of the current in-memory map.
     *
     * - The returned map is an unmodifiable view of the current key -> siblings mapping.
     * - The underlying lists are not deep-copied; callers must treat them as read-only.
     *
     * This is intended for read-only analytics / anti-entropy (Merkle snapshot).
     */
    public Map<String, List<VersionedValue>> snapshotAll() {
        return Map.copyOf(mem);
    }

}
