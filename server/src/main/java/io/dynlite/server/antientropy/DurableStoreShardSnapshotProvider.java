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
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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

        out.sort((a, b) -> Long.compareUnsigned(a.token(), b.token()));
        return out;
    }

    /**
     * Build a stable digest for a key and its sibling set.
     *
     * IMPORTANT: siblings are sorted deterministically so that the same logical
     * set of versions produces the same digest on all replicas, regardless of
     * insertion order in DurableStore.
     */
    private static byte[] digestKeyAndSiblings(
            MessageDigest md,
            String key,
            List<VersionedValue> siblings
    ) {
        md.reset();
        md.update(key.getBytes(StandardCharsets.UTF_8));

        // Deterministic ordering of siblings:
        //   1) by lwwMillis
        //   2) tombstone flag
        //   3) value bytes (lexicographically)
        List<VersionedValue> sorted = new ArrayList<>(siblings);
        sorted.sort(Comparator
                .comparingLong(VersionedValue::lwwMillis)
                .thenComparing(VersionedValue::tombstone)
                .thenComparing(DurableStoreShardSnapshotProvider::compareValueBytes));

        for (VersionedValue v : sorted) {
            // tombstone is part of identity
            md.update((byte) (v.tombstone() ? 1 : 0));

            // IMPORTANT: do NOT hash lwwMillis or vector clock.
            // They differ across replicas for the same logical value.

            // value payload bytes, if present
            byte[] valueBytes = v.value();
            if (valueBytes != null && valueBytes.length > 0) {
                md.update(valueBytes);
            }
        }

        return md.digest();
    }


    private static int compareValueBytes(VersionedValue a, VersionedValue b) {
        byte[] av = a.value();
        byte[] bv = b.value();

        if (av == null && bv == null) return 0;
        if (av == null) return -1;
        if (bv == null) return 1;

        int min = Math.min(av.length, bv.length);
        for (int i = 0; i < min; i++) {
            int cmp = Byte.compare(av[i], bv[i]);
            if (cmp != 0) return cmp;
        }
        return Integer.compare(av.length, bv.length);
    }
}
