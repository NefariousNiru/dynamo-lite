// file: src/main/java/io/dynlite/server/shard/ShardDescriptor.java
package io.dynlite.server.shard;

import java.util.Objects;

/**
 * Logical shard owned by a particular node over a TokenRange.
 *
 * For now, a shard is:
 *  - shardId: stable identifier for logging / RPC.
 *  - ownerNodeId: node responsible for this shard.
 *  - range: TokenRange in the 64-bit ring.
 *
 * How shards are planned / assigned (from the hash ring) is handled
 * by a separate planner; this is just the value object.
 */
public final class ShardDescriptor {

    private final String shardId;
    private final String ownerNodeId;
    private final TokenRange range;

    public ShardDescriptor(String shardId, String ownerNodeId, TokenRange range) {
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.ownerNodeId = Objects.requireNonNull(ownerNodeId, "ownerNodeId");
        this.range = Objects.requireNonNull(range, "range");
    }

    public String shardId() {
        return shardId;
    }

    public String ownerNodeId() {
        return ownerNodeId;
    }

    public TokenRange range() {
        return range;
    }

    @Override
    public String toString() {
        return "Shard[" + shardId + "@" + ownerNodeId + " " + range + "]";
    }
}
