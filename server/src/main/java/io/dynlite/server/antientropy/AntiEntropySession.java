// file: server/src/main/java/io/dynlite/server/antientropy/AntiEntropySession.java
package io.dynlite.server.antientropy;

import io.dynlite.core.merkle.MerkleDiff;
import io.dynlite.core.merkle.MerkleTree;
import io.dynlite.server.shard.ShardDescriptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Single anti-entropy session between this node (local) and a peer for one shard.
 *
 * High-level protocol:
 *  1) Local builds Merkle tree for shard using snapshot provider.
 *  2) Local asks peer for its Merkle snapshot (root + leaf manifests).
 *  3) If roots equal: shard is in sync, stop.
 *  4) Else:
 *      - rebuild remote Merkle tree from peer manifests,
 *      - compute differing leaves via MerkleDiff,
 *      - compare token/digest pairs and decide repairs (push / pull).
 *
 * This class focuses on the structure and use of MerkleDiff; the actual repair
 * (applying changes to storage) is delegated to a RepairExecutor.
 */
public final class AntiEntropySession {

    private final ShardDescriptor shard;
    private final ShardSnapshotProvider snapshotProvider;
    private final AntiEntropyPeer peer;
    private final RepairExecutor repairExecutor;
    private final int leafCount;

    /**
     * Hook to actually apply repairs to local storage, and to optionally push
     * our versions to the peer.
     */
    public interface RepairExecutor {
        /**
         * Called with sets of tokens that need to be:
         *  - fetched from peer and applied locally (pullTokens),
         *  - sent to peer so it can update itself (pushTokens).
         *
         * Implementations will:
         *  - use existing replication / NodeClient pathways to push/pull,
         *  - use vector clocks to resolve conflicts when moving data.
         */
        void executeRepairs(ShardDescriptor shard,
                            List<Long> pullTokens,
                            List<Long> pushTokens);
    }

    public AntiEntropySession(
            ShardDescriptor shard,
            ShardSnapshotProvider snapshotProvider,
            AntiEntropyPeer peer,
            RepairExecutor repairExecutor,
            int leafCount
    ) {
        this.shard = shard;
        this.snapshotProvider = snapshotProvider;
        this.peer = peer;
        this.repairExecutor = repairExecutor;
        this.leafCount = leafCount;
    }

    /**
     * Run a single anti-entropy round for this shard against the peer.
     *
     * @return true if the shard is already in sync (roots equal), false otherwise.
     */
    public boolean runOnce() {
        // 1) Build local Merkle tree from snapshot.
        Iterable<MerkleTree.KeyDigest> localSnapshot = snapshotProvider.snapshot(shard);
        MerkleTree localTree = MerkleTree.build(localSnapshot, leafCount);

        // 2) Ask peer for its Merkle snapshot (root + manifests).
        AntiEntropyPeer.MerkleSnapshotResponse remoteSnap =
                peer.fetchMerkleSnapshot(shard, leafCount);

        // 3) Compare roots.
        if (java.util.Arrays.equals(localTree.root(), remoteSnap.rootHash())) {
            // Shard already in sync; nothing to do.
            return true;
        }

        // 4) Roots differ. Rebuild remote Merkle tree from peer manifests.
        Iterable<MerkleTree.KeyDigest> remoteDigests =
                flattenManifests(remoteSnap.manifests());

        MerkleTree remoteTree = MerkleTree.build(remoteDigests, leafCount);

        // 5) Use MerkleDiff to find differing leaves between localTree and remoteTree.
        List<MerkleDiff.DifferingLeaf> diffs =
                MerkleDiff.findDifferingLeaves(localTree, remoteTree);

        // 6) From differing leaves, compute which tokens to pull from peer vs push.
        List<Long> pullTokens = new ArrayList<>();
        List<Long> pushTokens = new ArrayList<>();

        for (MerkleDiff.DifferingLeaf d : diffs) {
            MerkleTree.LeafManifest localLeaf = d.left();
            MerkleTree.LeafManifest remoteLeaf = d.right();

            // Build maps token -> digest for quick comparison.
            Map<Long, byte[]> localMap = toMap(localLeaf);
            Map<Long, byte[]> remoteMap = toMap(remoteLeaf);

            // Tokens present remotely but missing or outdated locally -> pull.
            for (var e : remoteMap.entrySet()) {
                long token = e.getKey();
                byte[] remoteDigest = e.getValue();
                byte[] localDigest = localMap.get(token);
                if (localDigest == null || !java.util.Arrays.equals(localDigest, remoteDigest)) {
                    pullTokens.add(token);
                }
            }

            // Tokens present locally but missing or outdated remotely -> push.
            for (var e : localMap.entrySet()) {
                long token = e.getKey();
                byte[] localDigest = e.getValue();
                byte[] remoteDigest = remoteMap.get(token);
                if (remoteDigest == null || !java.util.Arrays.equals(localDigest, remoteDigest)) {
                    pushTokens.add(token);
                }
            }
        }

        // 7) Delegate actual repair to caller.
        if (!pullTokens.isEmpty() || !pushTokens.isEmpty()) {
            repairExecutor.executeRepairs(shard, pullTokens, pushTokens);
        }

        return false;
    }

    // ---------- helpers ----------

    private static Iterable<MerkleTree.KeyDigest> flattenManifests(
            Map<Integer, MerkleTree.LeafManifest> manifests
    ) {
        List<MerkleTree.KeyDigest> all = new ArrayList<>();
        for (MerkleTree.LeafManifest m : manifests.values()) {
            all.addAll(m.entries());
        }
        // In practice, these should already be sorted by token; if not, sort here.
        all.sort(java.util.Comparator.comparingLong(MerkleTree.KeyDigest::token));
        return all;
    }

    private static Map<Long, byte[]> toMap(MerkleTree.LeafManifest m) {
        Map<Long, byte[]> map = new HashMap<>();
        for (MerkleTree.KeyDigest kd : m.entries()) {
            map.put(kd.token(), kd.digest());
        }
        return map;
    }
}
