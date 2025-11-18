// file: src/main/java/io/dynlite/core/merkle/MerkleDiff.java
package io.dynlite.core.merkle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Utilities for diffing two Merkle trees with the same shape (same leafCount).
 * Used by anti-entropy:
 *  - If root hashes differ, recursively descend until we reach leaves.
 *  - For each differing leaf, return its leaf manifest on both sides.
 * Assumes both trees are SimpleMerkle instances built with the same leafCount
 * and the same ordering / bucketing rules.
 */
public final class MerkleDiff {

    private MerkleDiff() {}

    /** Description of a single differing leaf between two trees. */
    public record DifferingLeaf(
            int leafNodeId,
            MerkleTree.LeafManifest left,
            MerkleTree.LeafManifest right
    ) {}

    /**
     * Compare two trees and return all leaf node ids whose hashes differ.
     * If root hashes are equal, the list is empty by definition (trees are identical).
     */
    public static List<DifferingLeaf> findDifferingLeaves(MerkleTree left, MerkleTree right) {
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");

        if (!(left instanceof SimpleMerkle l) || !(right instanceof SimpleMerkle r)) {
            throw new IllegalArgumentException("MerkleDiff currently supports SimpleMerkle trees only");
        }

        List<DifferingLeaf> out = new ArrayList<>();

        // Quick exit: identical roots => identical trees for our purposes.
        if (Arrays.equals(l.root(), r.root())) {
            return out;
        }

        diffNode(0, l, r, out); // start at root nodeId=0
        return out;
    }

    private static void diffNode(
            int nodeId,
            SimpleMerkle left,
            SimpleMerkle right,
            List<DifferingLeaf> out
    ) {
        byte[] lh = left.hashAt(nodeId);
        byte[] rh = right.hashAt(nodeId);

        if (Arrays.equals(lh, rh)) {
            // Subtree identical, no need to descend.
            return;
        }

        if (left.isLeaf(nodeId)) {
            // Leaf hashes differ: add the leaf manifests from both sides.
            MerkleTree.LeafManifest lm = left.manifest(nodeId);
            MerkleTree.LeafManifest rm = right.manifest(nodeId);
            out.add(new DifferingLeaf(nodeId, lm, rm));
            return;
        }

        // Recurse into children (binary tree).
        diffNode(left.leftChild(nodeId), left, right, out);
        diffNode(left.rightChild(nodeId), left, right, out);
    }
}
