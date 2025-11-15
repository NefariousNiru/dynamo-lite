// file: src/main/java/io/dynlite/core/merkle/MerkleTree.java
package io.dynlite.core.merkle;

import java.util.List;


/**
 * Abstraction for a binary Merkle tree built over a shard of keys.
 * <p>
 * At a high level:
 *  - The shard is partitioned into a fixed number of leaf buckets.
 *  - Each bucket holds a LeafManifest: a list of (token, digest) pairs.
 *  - Each leaf is hashed into a single hash.
 *  - Internal nodes hash their two child hashes.
 * <p>
 * This interface is intentionally small:
 *  - root():     used to quickly check if two replicas are identical.
 *  - children(): used to recursively find the first differing subtree.
 *  - manifest(): used to inspect leaf contents when reconciliation is needed.
 */
public interface MerkleTree {

    /** Root hash bytes (for example SHA-256).
     * Identical roots imply identical shard snapshot.
     * Time: O(1)
     * Space: O(1)
     * */
    byte[] root();

    /**
     * Return child hashes for the given node id in the implicit binary tree layout.
     * Node indexing:
     *  - root has id 0,
     *  - children of node n are at 2n+1 (left) and 2n+2 (right),
     *  - leaves form the last level of the tree.
     * For leaf nodes, this returns an empty array.
     * Time: O(1)
     * Space: O(1)
     */
    byte[][] children(int nodeId);

    /**
     * Leaf manifest for the given leaf node id.
     * This is only valid when nodeId refers to a leaf. For internal nodes,
     * callers must not call manifest and are expected to recurse further down.
     */
    LeafManifest manifest(int leafNodeId);

    /**
     * Build a Merkle tree from a sorted sequence of KeyDigest entries.
     * The iterable must be sorted by token ascending using unsigned comparison.
     * Leaf count must be a power of two; it defines the number of buckets.
     * O(n) to build, O(log n) to compare
     */
    static MerkleTree build(Iterable<KeyDigest> sortedByToken, int leafCount) {
        return new SimpleMerkle(sortedByToken, leafCount); // placeholder impl class
    }

    /**
     * Per-key digest entry used to build leaves.
     * - token: 64-bit hash of the key (same space as the hash ring).
     * - digest: hash of the key's full state (key + value + clock etc.).
     */
    record KeyDigest(long token, byte[] digest) {}

    /**
     * Leaf contents for reconciliation: the full list of keys that landed
     * in this bucket.
     */
    record LeafManifest(List<KeyDigest> entries) {}
}
