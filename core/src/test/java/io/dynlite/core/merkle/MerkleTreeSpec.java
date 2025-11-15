package io.dynlite.core.merkle;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MerkleTreeSpec {

    private static MerkleTree.KeyDigest kd(long token, String data) {
        return new MerkleTree.KeyDigest(token, SimpleMerkle.h(longToBytes(token), data.getBytes()));
    }
    private static byte[] longToBytes(long v) {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(v).array();
    }

    @Test
    void equal_datasets_equal_roots() {
        var data = new ArrayList<MerkleTree.KeyDigest>(List.of(
                kd(10,"A"), kd(20,"B"), kd(30,"C"), kd(40,"D")));
        data.sort(Comparator.comparingLong(MerkleTree.KeyDigest::token));
        var t1 = MerkleTree.build(data, 8);
        var t2 = MerkleTree.build(data, 8);
        assertArrayEquals(t1.root(), t2.root());
    }

    @Test
    void single_key_change_affects_one_leaf_only() {
        var left = new ArrayList<MerkleTree.KeyDigest>(List.of(
                kd(10,"A"), kd(20,"B"), kd(30,"C"), kd(40,"D")));
        var right = new ArrayList<MerkleTree.KeyDigest>(List.of(
                kd(10,"A"), kd(20,"B"), kd(30,"C"), kd(40,"X"))); // D -> X for token 40
        left.sort(Comparator.comparingLong(MerkleTree.KeyDigest::token));
        right.sort(Comparator.comparingLong(MerkleTree.KeyDigest::token));
        var L = MerkleTree.build(left, 8);
        var R = MerkleTree.build(right, 8);

        assertNotEquals(hex(L.root()), hex(R.root()));
        // You will implement a small helper to walk down and find the first leaf id that differs,
        // then assert only that leaf manifest differs while others match.
    }

    @Test
    void multiple_keys_in_different_leaves_diff_multiple_manifests() {
        // Similar to above: change tokens that land in different buckets; expect >1 differing leaves.
    }

    private static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (byte x : b) sb.append(String.format("%02x", x));
        return sb.toString();
    }
}
