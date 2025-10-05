package io.dynlite.storage;

import io.dynlite.core.VersionedValue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
        // 2) append + fsync
        wal.append(payload);
        // 3) apply in memory
        if (dedupe.firstTime(opId)) {
            applyToMemory(key, value);
        }
        // 4) maybe rotate and maybe snapshot on thresholds
        wal.rotateIfNeeded();
        snapPolicy.maybeSnapshot(mem, snaps);
    }

    @Override
    public VersionedValue get(String key) { return mem.get(key); }

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

    private void applyToMemory(String key, VersionedValue val) {
        if (val.tombstone()) mem.remove(key);
        else mem.put(key, val);
    }
}
