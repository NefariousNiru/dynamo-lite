// file: server/src/main/java/io/dynlite/server/antientropy/AntiEntropyPeer.java
package io.dynlite.server.antientropy;

import io.dynlite.core.merkle.MerkleTree;
import io.dynlite.server.shard.ShardDescriptor;

import java.util.Map;

/**
 * RPC-style interface for talking to a peer during anti-entropy.
 * Implementation can be:
 *  - gRPC client,
 *  - HTTP client,
 *  - in-process fake for tests.
 * For now we expose a single "Merkle snapshot" call that returns:
 *  - root hash,
 *  - leafCount,
 *  - per-leaf manifests (nodeId -> LeafManifest).
 */
public interface AntiEntropyPeer {

    /**
     * Ask a peer to compute a Merkle snapshot for the given shard.
     * The peer is responsible for:
     *  - scanning its local storage for keys whose tokens fall in shard.range(),
     *  - building a MerkleTree over those keys with the given leafCount,
     *  - returning the root hash and leaf manifests.
     */
    MerkleSnapshotResponse fetchMerkleSnapshot(ShardDescriptor shard, int leafCount);

    // -------- message type --------

    record MerkleSnapshotResponse(
            ShardDescriptor shard,
            byte[] rootHash,
            int leafCount,
            Map<Integer, MerkleTree.LeafManifest> manifests
    ) {}
}
