// file: src/main/java/io/dynlite/server/antientropy/ShardSnapshotProvider.java
package io.dynlite.server.antientropy;

import io.dynlite.core.merkle.MerkleTree;
import io.dynlite.server.shard.ShardDescriptor;

/**
 * Provides a stable snapshot of the keys in a shard, for Merkle tree construction.
 *
 * Implementation should:
 *  - iterate over all keys for which token(key) is in shard.range(),
 *  - produce MerkleTree.KeyDigest entries sorted by token.
 */
public interface ShardSnapshotProvider {

    /**
     * Take a snapshot of the shard's contents and return a stream of KeyDigest entries.
     *
     * Implementations should ensure a consistent point-in-time view suitable for Merkle comparison
     * (e.g., by reading from a snapshot or using appropriate locking).
     */
    Iterable<MerkleTree.KeyDigest> snapshot(ShardDescriptor shard);
}
