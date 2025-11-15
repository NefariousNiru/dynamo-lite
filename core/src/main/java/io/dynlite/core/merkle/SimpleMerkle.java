package io.dynlite.core.merkle;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Binary Merkle tree with fixed leaf count (power of two). Deterministic and simple. */
final class SimpleMerkle implements MerkleTree {
    private static final int HASH_LEN = 32; // SHA-256
    private final int leafCount;            // power of two
    private final int totalNodes;           // 2*leafCount - 1
    private final byte[][] nodeHash;        // nodeId -> hash
    private final LeafManifest[] leafManifests; // only for leaves (node ids: base..base+leafCount-1)
    private final int baseLeafId;           // node id of the first leaf

    SimpleMerkle(Iterable<KeyDigest> sortedByToken, int leafCount) {
        if (leafCount <= 0 || (leafCount & (leafCount - 1)) != 0)
            throw new IllegalArgumentException("leafCount must be a power of two");
        this.leafCount = leafCount;
        this.totalNodes = (leafCount << 1) - 1;
        this.nodeHash = new byte[totalNodes][];
        this.leafManifests = new LeafManifest[leafCount];
        this.baseLeafId = leafBase();

        // 1) bucket entries into leaves
        @SuppressWarnings("unchecked")
        List<KeyDigest>[] buckets = new List[leafCount];
        for (int i = 0; i < leafCount; i++) buckets[i] = new ArrayList<>();

        for (KeyDigest kd : sortedByToken) {
            Objects.requireNonNull(kd, "KeyDigest");
            int idx = bucketFor(kd.token());
            buckets[idx].add(kd);
        }

        // 2) build leaves: manifest + hash concat(token||digest)
        for (int i = 0; i < leafCount; i++) {
            var manifest = new LeafManifest(List.copyOf(buckets[i]));
            leafManifests[i] = manifest;

            var md = newDigest();
            for (var e : buckets[i]) {
                md.update(longBE(e.token()));   // token as 8 bytes big-endian
                md.update(e.digest());          // already a hash of key/value/clock etc.
            }
            nodeHash[baseLeafId + i] = md.digest();
        }

        // 3) build parents upward: parent = H(left || right)
        for (int n = baseLeafId - 1; n >= 0; n--) {
            int left = leftChild(n), right = rightChild(n);
            nodeHash[n] = h(nodeHash[left], nodeHash[right]);
        }
    }

    @Override public byte[] root() { return nodeHash[0]; }

    @Override public byte[][] children(int nodeId) {
        if (nodeId < 0 || nodeId >= totalNodes) throw new IllegalArgumentException("bad nodeId");
        if (isLeaf(nodeId)) return new byte[0][];
        return new byte[][] { nodeHash[leftChild(nodeId)], nodeHash[rightChild(nodeId)] };
    }

    @Override public LeafManifest manifest(int leafNodeId) {
        if (!isLeaf(leafNodeId)) throw new IllegalArgumentException("not a leaf node id");
        int leafIdx = leafNodeId - baseLeafId;
        return leafManifests[leafIdx];
    }

    // ---------------- helpers ----------------

    private int leafBase() { return leafCount - 1; } // root=0, leaves start at index (leafCount - 1)
    private boolean isLeaf(int nodeId) { return nodeId >= baseLeafId; }
    private int leftChild(int n) { return (n << 1) + 1; }
    private int rightChild(int n) { return (n << 1) + 2; }

    /** Map unsigned 64-bit token into [0, leafCount) evenly. */
    private int bucketFor(long token) {
        // leafCount is 2^k => we want the top k bits of 'token' as the bucket index.
        int k = Integer.numberOfTrailingZeros(leafCount); // since leafCount is power of two
        int shift = 64 - k;
        return (int) (token >>> shift); // '>>>' is unsigned shift for long
    }


    static byte[] h(byte[] a, byte[] b) {
        var md = newDigest();
        md.update(a == null ? new byte[HASH_LEN] : a);
        md.update(b == null ? new byte[HASH_LEN] : b);
        return md.digest();
    }

    static byte[] h(byte[]... parts) {
        var md = newDigest();
        for (var p : parts) md.update(p);
        return md.digest();
    }

    static byte[] longBE(long v) {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(v).array();
    }

    static MessageDigest newDigest() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (Exception e) { throw new RuntimeException(e); }
    }
}
