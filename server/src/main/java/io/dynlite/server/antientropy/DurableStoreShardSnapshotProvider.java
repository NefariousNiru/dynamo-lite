// file: server/src/main/java/io/dynlite/server/antientropy/DurableStoreShardSnapshotProvider.java
package io.dynlite.server.antientropy;

import io.dynlite.core.HashRing;
import io.dynlite.core.VersionedValue;
import io.dynlite.core.merkle.MerkleTree;
import io.dynlite.server.shard.ShardDescriptor;
import io.dynlite.storage.DurableStore;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ShardSnapshotProvider implementation backed by DurableStore.
 *
 * Responsibilities:
 *  - Iterate over all keys in the DurableStore.
 *  - For each key, compute its token using the same hash as the HashRing.
 *  - Filter keys whose token falls into shard.range().
 *  - For each (key, siblings) pair, compute a stable digest that captures:
 *      * tombstone flag,
 *      * lwwMillis,
 *      * vector clock entries,
 *      * value payload bytes.
 *  - Return a sorted list of MerkleTree.KeyDigest entries.
 *
 * This enables MerkleTree.build(...) to reconstruct the same structure
 * on all replicas for the same shard.
 */
public final class DurableStoreShardSnapshotProvider implements ShardSnapshotProvider {

    private final DurableStore store;

    public DurableStoreShardSnapshotProvider(DurableStore store) {
        this.store = store;
    }

    @Override
    public Iterable<MerkleTree.KeyDigest> snapshot(ShardDescriptor shard) {
        Map<String, List<VersionedValue>> all = store.snapshotAll();
        List<MerkleTree.KeyDigest> out = new ArrayList<>(all.size());

        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (Exception e) {
            throw new RuntimeException("Unable to create SHA-256 MessageDigest", e);
        }

        var range = shard.range();

        for (Map.Entry<String, List<VersionedValue>> e : all.entrySet()) {
            String key = e.getKey();
            List<VersionedValue> siblings = e.getValue();
            if (siblings == null || siblings.isEmpty()) {
                continue;
            }

            long token = HashRing.tokenForKey(key);
            if (!range.contains(token)) {
                continue;
            }

            byte[] digest = digestKeyAndSiblings(md, key, siblings);
            out.add(new MerkleTree.KeyDigest(token, digest));
        }

        // MerkleTree expects entries sorted by token.
        out.sort((a, b) -> Long.compareUnsigned(a.token(), b.token()));
        return out;
    }

    /**
     * Build a stable digest for a key and its sibling set.
     *
     * Layout is arbitrary but must be deterministic:
     *   H( key || sibling_1 || sibling_2 || ... )
     *
     * Each sibling folds in:
     *   - tombstone flag,
     *   - lwwMillis,
     *   - vector clock entries sorted by nodeId,
     *   - value bytes (if present).
     */
    private static byte[] digestKeyAndSiblings(
            MessageDigest md,
            String key,
            List<VersionedValue> siblings
    ) {
        md.reset();
        md.update(key.getBytes(StandardCharsets.UTF_8));

        for (VersionedValue v : siblings) {
            // tombstone
            md.update((byte) (v.tombstone() ? 1 : 0));

            // lwwMillis
            ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN);
            buf.putLong(0, v.lwwMillis());
            md.update(buf);

            // vector clock entries sorted by nodeId
            var clock = v.clock();
            if (clock != null) {
                Map<String, Integer> clockEntries = clock.entries();
                if (!clockEntries.isEmpty()) {
                    clockEntries.entrySet().stream()
                            .sorted(Map.Entry.comparingByKey())
                            .forEach(entry -> {
                                md.update(entry.getKey().getBytes(StandardCharsets.UTF_8));
                                ByteBuffer cbuf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN);
                                cbuf.putLong(0, entry.getValue());
                                md.update(cbuf);
                            });
                }
            }

            // value payload bytes, if present
            byte[] valueBytes = v.value();
            if (valueBytes != null && valueBytes.length > 0) {
                md.update(valueBytes);
            }
        }

        return md.digest();
    }
}

