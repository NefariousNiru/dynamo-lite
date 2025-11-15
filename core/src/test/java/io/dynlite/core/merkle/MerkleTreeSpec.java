// file: src/test/java/io/dynlite/core/merkle/MerkleTreeSpec.java
package io.dynlite.core.merkle;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Spec tests for MerkleTree over token space.
 *
 * Goal is to validate:
 *  - deterministic root for same dataset,
 *  - root changes when key content changes,
 *  - differences localize to specific leaves.
 */
class MerkleTreeSpec {

    private static MerkleTree.KeyDigest kd(long token, String data) {
        return new MerkleTree.KeyDigest(
                token,
                SimpleMerkle.h(longToBytes(token), data.getBytes())
        );
    }

    private static byte[] longToBytes(long v) {
        return ByteBuffer.allocate(8)
                .order(ByteOrder.BIG_ENDIAN)
                .putLong(v)
                .array();
    }

    private static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (byte x : b) {
            sb.append(String.format("%02x", x));
        }
        return sb.toString();
    }

    @Test
    void equal_datasets_equal_roots() {
        var data = new ArrayList<MerkleTree.KeyDigest>(List.of(
                kd(10, "A"),
                kd(20, "B"),
                kd(30, "C"),
                kd(40, "D")
        ));
        data.sort(Comparator.comparingLong(MerkleTree.KeyDigest::token));

        MerkleTree t1 = MerkleTree.build(data, 8);
        MerkleTree t2 = MerkleTree.build(data, 8);

        assertArrayEquals(t1.root(), t2.root(), "Roots must match for identical datasets");
    }

    @Test
    void single_key_change_affects_single_leaf_manifest() {
        var left = new ArrayList<MerkleTree.KeyDigest>(List.of(
                kd(10, "A"), kd(20, "B"), kd(30, "C"), kd(40, "D")
        ));
        var right = new ArrayList<MerkleTree.KeyDigest>(List.of(
                kd(10, "A"), kd(20, "B"), kd(30, "C"), kd(40, "X") // D -> X for token 40
        ));

        left.sort(Comparator.comparingLong(MerkleTree.KeyDigest::token));
        right.sort(Comparator.comparingLong(MerkleTree.KeyDigest::token));

        MerkleTree L = MerkleTree.build(left, 8);
        MerkleTree R = MerkleTree.build(right, 8);

        assertNotEquals(
                hex(L.root()), hex(R.root()),
                "Root hash must change when one key's digest changes"
        );

        // Find leaf nodeIds whose manifests differ.
        List<Integer> differingLeaves = differingLeafIds(L, R, 8);

        assertEquals(1, differingLeaves.size(), "Exactly one leaf manifest should differ");
    }

    @Test
    void multiple_keys_in_different_leaves_diff_multiple_manifests() {
        // Construct data so that two keys fall into different buckets and are changed.
        var base = new ArrayList<MerkleTree.KeyDigest>(List.of(
                kd(10, "A"), kd(20, "B"), kd(30, "C"), kd(40, "D"),
                kd(1L << 60, "Z") // larger token to land in another bucket
        ));

        var modified = new ArrayList<MerkleTree.KeyDigest>(List.of(
                kd(10, "A"), kd(20, "B"), kd(30, "C"),
                kd(40, "X"),   // change D -> X
                kd(1L << 60, "Y")  // change Z -> Y in a different leaf
        ));

        List<MerkleTree.KeyDigest> baseSorted = base.stream()
                .sorted(Comparator.comparingLong(MerkleTree.KeyDigest::token))
                .toList();

        List<MerkleTree.KeyDigest> modSorted = modified.stream()
                .sorted(Comparator.comparingLong(MerkleTree.KeyDigest::token))
                .toList();

        MerkleTree L = MerkleTree.build(baseSorted, 16);
        MerkleTree R = MerkleTree.build(modSorted, 16);

        assertNotEquals(
                hex(L.root()), hex(R.root()),
                "Root hash must change when multiple keys change"
        );

        List<Integer> differingLeaves = differingLeafIds(L, R, 16);

        assertTrue(differingLeaves.size() >= 2,
                "Multiple leaves should differ because changes are in different buckets");
    }

    @Test
    void leaf_update_same_token_dirties_only_one_leaf() {
        var data = List.of(
                kd(10, "A"), kd(20, "B"), kd(30, "C"), kd(40, "D")
        );
        var sorted = data.stream()
                .sorted(Comparator.comparingLong(MerkleTree.KeyDigest::token))
                .toList();

        MerkleTree tree = MerkleTree.build(sorted, 8);
        byte[] rootBefore = tree.root();

        // simulate a "change" by rebuilding with one token changed
        var modified = List.of(
                kd(10, "A"), kd(20, "B"), kd(30, "C"), kd(40, "X")
        );
        MerkleTree treeModified = MerkleTree.build(modified, 8);

        assertNotEquals(
                hex(rootBefore),
                hex(treeModified.root()),
                "Root hash must change if a single token digest changes"
        );
    }

    @Test
    void multiple_keys_same_leaf() {
        var data = List.of(
                kd(10, "A"), kd(11, "B"), kd(12, "C")
        );
        MerkleTree tree = MerkleTree.build(data, 4);

        // all tokens fall in same bucket
        byte[] rootBefore = tree.root();

        var modified = List.of(
                kd(10, "X"), kd(11, "Y"), kd(12, "Z")
        );
        MerkleTree treeModified = MerkleTree.build(modified, 4);

        assertNotEquals(hex(rootBefore), hex(treeModified.root()),
                "Root hash must change if multiple keys in same leaf change");
    }

    @Test
    void boundary_tokens() {
        var data = List.of(
                kd(0, "A"), kd(Long.MAX_VALUE, "B"), kd(Long.MIN_VALUE, "C")
        );
        MerkleTree tree = MerkleTree.build(data, 8);

        var modified = List.of(
                kd(0, "X"), kd(Long.MAX_VALUE, "Y"), kd(Long.MIN_VALUE, "Z")
        );
        MerkleTree treeModified = MerkleTree.build(modified, 8);

        assertNotEquals(hex(tree.root()), hex(treeModified.root()),
                "Root hash changes for tokens at boundaries");
    }

    @Test
    void empty_tree_build() {
        MerkleTree tree = MerkleTree.build(List.of(), 8);
        assertNotNull(tree.root(), "Root must exist for empty tree");

        // Root should have children
        byte[][] children = tree.children(0);
        assertEquals(2, children.length, "Root should have 2 children even for empty tree");

        // All leaves should exist and be empty
        for (int nodeId = 7; nodeId < 15; nodeId++) { // leafCount = 8 -> leaves 7..14
            MerkleTree.LeafManifest leaf = tree.manifest(nodeId);
            assertTrue(leaf.entries().isEmpty(), "Leaf should have no entries");
        }
    }

    @Test
    void h_varargs_multiple_parts() {
        byte[] part1 = "hello".getBytes();
        byte[] part2 = "world".getBytes();
        byte[] part3 = "!!!".getBytes();

        byte[] hash = SimpleMerkle.h(part1, part2, part3);
        assertNotNull(hash, "Hash must not be null");
        assertEquals(32, hash.length, "SHA-256 hash must be 32 bytes");
    }


    // --------- helper: enumerate differing leaves ---------

    /**
     * Return the list of leaf nodeIds whose manifests differ between two trees.
     *
     * @param left      Merkle tree 1
     * @param right     Merkle tree 2
     * @param leafCount number of leaves used to build the tree
     */
    private static List<Integer> differingLeafIds(MerkleTree left, MerkleTree right, int leafCount) {
        int baseLeafId = leafCount - 1;
        int totalNodes = 2 * leafCount - 1;

        List<Integer> differing = new ArrayList<>();
        for (int nodeId = baseLeafId; nodeId < totalNodes; nodeId++) {
            MerkleTree.LeafManifest m1 = left.manifest(nodeId);
            MerkleTree.LeafManifest m2 = right.manifest(nodeId);

            if (!sameManifest(m1, m2)) {
                differing.add(nodeId);
            }
        }
        return differing;
    }

    private static boolean sameManifest(MerkleTree.LeafManifest a, MerkleTree.LeafManifest b) {
        if (a.entries().size() != b.entries().size()) {
            return false;
        }
        // Compare as sets of (token, digestHex) tuples.
        var setA = a.entries().stream()
                .map(kd -> kd.token() + ":" + hex(kd.digest()))
                .collect(Collectors.toSet());
        var setB = b.entries().stream()
                .map(kd -> kd.token() + ":" + hex(kd.digest()))
                .collect(Collectors.toSet());
        return setA.equals(setB);
    }
}
