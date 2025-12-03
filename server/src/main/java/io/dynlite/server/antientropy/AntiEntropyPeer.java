// file: server/src/main/java/io/dynlite/server/antientropy/AntiEntropyPeer.java
package io.dynlite.server.antientropy;

import io.dynlite.core.merkle.MerkleTree;
import io.dynlite.server.shard.ShardDescriptor;

import java.util.List;

/**
 * RPC-style interface for talking to a peer during anti-entropy.
 *
 * Implementations can be:
 *  - HTTP client,
 *  - gRPC client,
 *  - in-process fake for tests.
 *
 * A Merkle snapshot contains:
 *  - the Merkle tree root hash for the shard,
 *  - the leafCount used when constructing the tree,
 *  - the flat list of (token, digest) pairs used to build the tree.
 *
 * The caller is responsible for reconstructing the Merkle tree locally using
 * {@link MerkleTree#build(Iterable, int)} when needed.
 */
public interface AntiEntropyPeer {

    /**
     * Ask a peer to compute a Merkle snapshot for the given shard.
     *
     * The peer is responsible for:
     *  - scanning its local storage for keys whose tokens fall in shard.range(),
     *  - computing a digest per token,
     *  - building a Merkle tree using the given leafCount,
     *  - returning the Merkle root hash and the underlying (token, digest) pairs.
     *
     * @param shard     shard to snapshot (range is taken from shard.range()).
     * @param leafCount number of leaves to use when building the Merkle tree.
     */
    MerkleSnapshotResponse fetchMerkleSnapshot(ShardDescriptor shard, int leafCount);

    /**
     * Snapshot of a peer's Merkle tree for a shard.
     *
     * @param shard     the shard description (copied from caller for convenience)
     * @param rootHash  Merkle root hash (byte[])
     * @param leafCount number of leaves used when building the tree
     * @param digests   flat list of token/digest pairs used to build the tree
     */
    record MerkleSnapshotResponse(
            ShardDescriptor shard,
            byte[] rootHash,
            int leafCount,
            List<MerkleTree.KeyDigest> digests
    ) {}
}
