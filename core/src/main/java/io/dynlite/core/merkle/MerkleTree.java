package io.dynlite.core.merkle;

import java.util.List;

/**
 * Binary Merkle tree over a stable snapshot of a shard, built from fixed-count leaf buckets.
 * Leaves hold manifests of (token,digest) pairs; parents hash child hashes.
 */
public interface MerkleTree {
    /** Root hash bytes (e.g., SHA-256). Identical roots => identical shard snapshot. */
    byte[] root();

    /** Child hashes for the given node id (0 is root). */
    byte[][] children(int nodeId);

    /** Leaf manifest (token,digest pairs) for a leaf node. */
    LeafManifest manifest(int leafNodeId);

    /** Build a tree from sorted key digests into 'leafCount' buckets. */
    static MerkleTree build(Iterable<KeyDigest> sortedByToken, int leafCount) {
        return new SimpleMerkle(sortedByToken, leafCount); // placeholder impl class
    }

    /** Per-key digest entry used to build leaves. */
    record KeyDigest(long token, byte[] digest) {}

    /** Leaf contents for reconciliation. */
    record LeafManifest(List<KeyDigest> entries) {}
}
