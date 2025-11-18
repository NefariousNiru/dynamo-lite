// file: server/src/test/java/io/dynlite/server/shard/TokenRangeSpec.java
package io.dynlite.server.shard;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Specs for TokenRange semantics over unsigned 64-bit space.
 */
class TokenRangeSpec {

    @Test
    void non_wrapping_range_contains_only_tokens_in_interval() {
        // [10, 20) in unsigned space
        TokenRange r = new TokenRange(10L, 20L);

        assertTrue(r.contains(10L), "start should be included");
        assertTrue(r.contains(15L), "middle should be included");
        assertTrue(r.contains(19L), "end-1 should be included");

        assertFalse(r.contains(20L), "end should be excluded");
        assertFalse(r.contains(9L), "below start should be excluded");
        assertFalse(r.contains(25L), "above end should be excluded");
    }

    @Test
    void wrapping_range_covers_tail_and_head_of_ring() {
        // [100, 20) wraps: covers [100, 2^64) U [0, 20)
        TokenRange r = new TokenRange(100L, 20L);

        assertTrue(r.contains(100L), "start should be included in wrapping range");
        assertTrue(r.contains(200L), "token >= start should be included");
        assertTrue(r.contains(Long.MAX_VALUE), "high-end tokens should be included");
        assertTrue(r.contains(0L), "0 should be included because range wraps");
        assertTrue(r.contains(10L), "tokens < end should be included in wrapping range");

        assertFalse(r.contains(50L), "tokens strictly between end and start should be excluded");
    }

    @Test
    void full_ring_when_start_equals_end_contains_everything() {
        // [x, x) by convention treated as full ring.
        TokenRange r = new TokenRange(0x1234_5678_9ABCL, 0x1234_5678_9ABCL);

        assertTrue(r.contains(0L));
        assertTrue(r.contains(1L));
        assertTrue(r.contains(Long.MAX_VALUE));
        assertTrue(r.contains(0xFFFF_FFFF_FFFFL));
    }

    @Test
    void shard_descriptor_holds_owner_and_range() {
        TokenRange r = new TokenRange(10L, 20L);
        ShardDescriptor shard = new ShardDescriptor("shard-1", "node-a", r);

        assertEquals("shard-1", shard.shardId());
        assertEquals("node-a", shard.ownerNodeId());
        assertSame(r, shard.range());
        assertTrue(shard.toString().contains("shard-1"));
        assertTrue(shard.toString().contains("node-a"));
    }
}
