// file: server/src/main/java/io/dynlite/server/antientropy/DurableStoreShardSnapshotProvider.java
package io.dynlite.server.antientropy;

import io.dynlite.core.HashRing;
import io.dynlite.core.VersionedValue;
import io.dynlite.core.VectorClock;
import io.dynlite.core.merkle.MerkleTree;
import io.dynlite.server.shard.ShardDescriptor;
import io.dynlite.storage.DurableStore;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * ShardSnapshotProvider backed by a DurableStore.
 *
 * For now, this implementation:
 *  - Takes a read-only snapshot of the in-memory map via DurableStore.snapshotAll().
 *  - Hashes each (key, value vector clock / tombstone / payload) into a digest.
 *  - Uses HashRing.tokenForKey(key) to map keys into the same 64-bit space as the ring.
 *
 * NOTE:
 *  - We currently ignore shard.range() and simply include all keys. This keeps the
 *    implementation simple and correct while you only have a single shard per node.
 *  - Once you have a shard planner, you can add filtering by token ∈ shard.range().
 */
public final class DurableStoreShardSnapshotProvider implements ShardSnapshotProvider {

    private final DurableStore store;

    public DurableStoreShardSnapshotProvider(DurableStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    @Override
    public Iterable<MerkleTree.KeyDigest> snapshot(ShardDescriptor shard) {
        Map<String, List<VersionedValue>> snap = store.snapshotAll();

        List<MerkleTree.KeyDigest> out = new ArrayList<>(snap.size());
        MessageDigest md = newSha256();

        for (Map.Entry<String, List<VersionedValue>> e : snap.entrySet()) {
            String key = e.getKey();
            List<VersionedValue> siblings = e.getValue();
            if (siblings == null || siblings.isEmpty()) {
                continue;
            }

            byte[] digest = digestKeyAndSiblings(md, key, siblings);
            long token = HashRing.tokenForKey(key);

            out.add(new MerkleTree.KeyDigest(token, digest));
        }

        // MerkleTree.build() expects entries sorted by token.
        out.sort((a, b) -> Long.compareUnsigned(a.token(), b.token()));
        return out;
    }

    private static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (Exception e) {
            throw new RuntimeException("Unable to get SHA-256 MessageDigest", e);
        }
    }

    /**
     * Build a stable digest for a key and its sibling set.
     * Layout is arbitrary but must be deterministic:
     *   H( key || sibling_1 || sibling_2 || ... ).
     *
     * To avoid depending on internal VectorClock structure, we just fold
     * in clock.toString(), which is deterministic as long as VectorClock's
     * toString is deterministic for equal clocks on all nodes.
     */
    private static byte[] digestKeyAndSiblings(MessageDigest md, String key, List<VersionedValue> siblings) {
        md.reset();
        md.update(key.getBytes(StandardCharsets.UTF_8));

        for (VersionedValue v : siblings) {
            // tombstone flag
            md.update((byte) (v.tombstone() ? 1 : 0));

            // lwwMillis
            ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
            buf.putLong(0, v.lwwMillis());
            md.update(buf);

            // vector clock (opaque but deterministic text form)
            VectorClock clock = v.clock();
            if (clock != null) {
                md.update(clock.toString().getBytes(StandardCharsets.UTF_8));
            }

            // raw value bytes
            byte[] val = v.value();
            if (val != null) {
                md.update(val);
            }
        }

        return md.digest();
    }
}
