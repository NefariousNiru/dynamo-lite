// file: server/src/main/java/io/dynlite/server/antientropy/DurableStoreShardSnapshotProvider.java
package io.dynlite.server.antientropy;

import io.dynlite.core.HashRing;
import io.dynlite.core.merkle.MerkleTree;
import io.dynlite.core.VersionedValue;
import io.dynlite.server.shard.ShardDescriptor;
import io.dynlite.server.shard.TokenRange;
import io.dynlite.storage.DurableStore;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * ShardSnapshotProvider backed by a DurableStore.
 *
 * Responsibilities:
 *  - Iterate over all keys in the DurableStore.
 *  - Hash each key into a 64-bit token using the same function as HashRing.
 *  - Filter keys whose token lies in shard.range().
 *  - For each such key, compute a digest that summarizes its sibling set.
 *  - Produce a list of MerkleTree.KeyDigest entries sorted by token.
 *
 * Notes:
 *  - This implementation uses SHA-256 over (key + siblings.toString()) as a simple,
 *    deterministic digest. For production, you may want a more explicit encoding
 *    of VersionedValue fields (clock, tombstone, value bytes).
 */
public final class DurableStoreShardSnapshotProvider implements ShardSnapshotProvider {

    private final DurableStore store;

    public DurableStoreShardSnapshotProvider(DurableStore store) {
        this.store = store;
    }

    @Override
    public Iterable<MerkleTree.KeyDigest> snapshot(ShardDescriptor shard) {
        TokenRange range = shard.range();
        Map<String, List<VersionedValue>> snapshot = store.snapshotAll();

        List<MerkleTree.KeyDigest> digests = new ArrayList<>();

        for (Map.Entry<String, List<VersionedValue>> e : snapshot.entrySet()) {
            String key = e.getKey();
            long token = HashRing.tokenForKey(key); // must match ring hashing

            if (!contains(range, token)) {
                continue;
            }

            List<VersionedValue> siblings = e.getValue();
            if (siblings == null || siblings.isEmpty()) {
                continue;
            }

            byte[] digestBytes = digestKeyAndSiblings(key, siblings);
            digests.add(new MerkleTree.KeyDigest(token, digestBytes));
        }

        // MerkleTree.build expects tokens in sorted order.
        digests.sort(Comparator.comparingLong(MerkleTree.KeyDigest::token));
        return digests;
    }

    private static boolean contains(TokenRange range, long token) {
        // If TokenRange already has a contains() method, you can use that instead.
        long start = range.startInclusive();
        long end = range.endExclusive();
        return token >= start && token < end;
    }

    /**
     * Compute a deterministic digest for a key's sibling set.
     *
     * For now:
     *   digest = SHA-256( key + "|" + siblings.toString() ).
     *
     * This is simple but good enough for anti-entropy:
     *  - identical key/sibling sets on different replicas produce identical digests,
     *  - any change in siblings will change the digest with high probability.
     */
    private static byte[] digestKeyAndSiblings(String key, List<VersionedValue> siblings) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(key.getBytes(StandardCharsets.UTF_8));
            md.update((byte) '|');
            md.update(siblings.toString().getBytes(StandardCharsets.UTF_8));
            return md.digest();
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed in the JDK; if this ever fails, fallback to a naive digest.
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
