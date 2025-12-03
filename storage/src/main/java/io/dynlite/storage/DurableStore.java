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
 *
 * Responsibilities:
 *  - Maintain an in-memory map: key -> VersionedValue.
 *  - On write:
 *      1) Serialize opId+key+value to a WAL record.
 *      2) Append+fsync to WAL.
 *      3) If deduper says this opId is new, apply it to memory.
 *      4) Rotate WAL segment if needed.
 *      5) Possibly trigger a full snapshot based on SnapshotPolicy.
 *
 *  - On startup:
 *      1) Load the latest snapshot (if any) into memory.
 *      2) Replay WAL records, applying each opId exactly once.
 *
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
        this.wal = wal;
        this.snaps = snaps;
        this.dedupe = dedupe;
        recover();
    }

    @Override
    public synchronized void put(String key, VersionedValue value, String opId) {
        var payload = RecordCodec.encode(opId, key, value);
        wal.append(payload);

        if (dedupe.firstTime(opId)) {
            applyToMemory(key, value);
        }

        wal.rotateIfNeeded();
        snapPolicy.maybeSnapshot(mem, snaps);
    }

    @Override
    public VersionedValue get(String key) {
        List<VersionedValue> siblings = mem.get(key);
        if (siblings == null || siblings.isEmpty()) {
            return null;
        }

        boolean hasLive = siblings.stream().anyMatch(v -> !v.tombstone());
        if (!hasLive) {
            return null;
        }

        if (siblings.size() == 1) {
            VersionedValue only = siblings.getFirst();
            return only.tombstone() ? null : only;
        }

        VersionedValue chosen = resolver.choose(siblings);
        return chosen.tombstone() ? null : chosen;
    }

    @Override
    public List<VersionedValue> getSiblings(String key) {
        List<VersionedValue> siblings = mem.get(key);
        if (siblings == null || siblings.isEmpty()) {
            return List.of();
        }
        return List.copyOf(siblings);
    }

    private void recover() {
        Snapshotter.LoadedSnapshot loaded = snaps.loadLatest();
        if (loaded != null && loaded.data() != null) {
            mem.putAll(loaded.data());
        }

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

    private void applyToMemory(String key, VersionedValue newVal) {
        List<VersionedValue> existing = mem.get(key);

        List<VersionedValue> candidates;
        if (existing == null || existing.isEmpty()) {
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

        mem.put(key, newSiblings);
    }

    /**
     * Return a shallow, read-only snapshot of the current in-memory map.
     *
     * The returned map and lists should be treated as immutable by callers.
     * This is intended for read-only operations such as Merkle tree
     * construction and anti-entropy snapshotting.
     */
    public Map<String, List<VersionedValue>> snapshotAll() {
        return Map.copyOf(mem);
    }
}
