// file: server/src/test/java/io/dynlite/server/antientropy/AntiEntropySessionSpec.java
package io.dynlite.server.antientropy;

import io.dynlite.core.merkle.MerkleTree;
import io.dynlite.server.shard.ShardDescriptor;
import io.dynlite.server.shard.TokenRange;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Specs for AntiEntropySession behavior with fake peer and snapshot provider.
 */
class AntiEntropySessionSpec {

    private static MerkleTree.KeyDigest kd(long token, byte[] digest) {
        return new MerkleTree.KeyDigest(token, digest);
    }

    @Test
    void runOnce_returns_true_when_roots_match_and_no_repairs() {
        int leafCount = 4;
        ShardDescriptor shard = new ShardDescriptor(
                "shard-1",
                "node-a",
                new TokenRange(0L, 0L) // full ring, doesn't matter here
        );

        // Shared snapshot for both local and remote.
        List<MerkleTree.KeyDigest> digests = new ArrayList<>(List.of(
                kd(10L, new byte[]{1}),
                kd(20L, new byte[]{2}),
                kd(30L, new byte[]{3})
        ));
        digests.sort(Comparator.comparingLong(MerkleTree.KeyDigest::token));

        // Local snapshot provider returns this fixed view.
        ShardSnapshotProvider snapshotProvider = s -> digests;

        // Pre-build a tree so we know the expected root for the fake peer.
        MerkleTree reference = MerkleTree.build(digests, leafCount);
        byte[] root = reference.root();

        // Fake peer that reports the same root and doesn't need to supply manifests,
        // because AntiEntropySession will short-circuit when roots are equal.
        AntiEntropyPeer peer = (requestShard, requestedLeafCount) ->
                new AntiEntropyPeer.MerkleSnapshotResponse(
                        requestShard,
                        root,
                        requestedLeafCount,
                        Map.of() // manifests unused when roots match
                );

        // Repair executor should never be invoked in this case.
        AtomicBoolean repaired = new AtomicBoolean(false);
        AntiEntropySession.RepairExecutor repairExec = (s, pull, push) -> repaired.set(true);

        AntiEntropySession session = new AntiEntropySession(
                shard,
                snapshotProvider,
                peer,
                repairExec,
                leafCount
        );

        boolean inSync = session.runOnce();
        assertTrue(inSync, "Session should report in-sync when roots match");
        assertFalse(repaired.get(), "No repairs should be executed when trees are equal");
    }

    @Test
    void runOnce_computes_pull_and_push_tokens_when_snapshots_differ() {
        int leafCount = 4;
        ShardDescriptor shard = new ShardDescriptor(
                "shard-1",
                "node-a",
                new TokenRange(0L, 0L)
        );

        // Local snapshot: tokens 10 and 20
        List<MerkleTree.KeyDigest> localDigests = new ArrayList<>(List.of(
                kd(10L, new byte[]{1}),
                kd(20L, new byte[]{2})
        ));
        localDigests.sort(Comparator.comparingLong(MerkleTree.KeyDigest::token));

        ShardSnapshotProvider snapshotProvider = s -> localDigests;

        // Remote snapshot: tokens 10 and 30
        List<MerkleTree.KeyDigest> remoteDigests = new ArrayList<>(List.of(
                kd(10L, new byte[]{1}), // same
                kd(30L, new byte[]{3})  // new on remote
        ));
        remoteDigests.sort(Comparator.comparingLong(MerkleTree.KeyDigest::token));

        // Fake peer: root MUST differ from local to force diff path.
        AntiEntropyPeer peer = (requestShard, requestedLeafCount) -> {
            // Build a tree just so rootHash is non-equal to local.
            MerkleTree remoteTree = MerkleTree.build(remoteDigests, requestedLeafCount);
            byte[] remoteRoot = remoteTree.root();

            // Put all remote digests into a single leaf manifest; AntiEntropySession
            // will flatten and rebuild its own remote tree anyway.
            MerkleTree.LeafManifest manifest = new MerkleTree.LeafManifest(remoteDigests);
            Map<Integer, MerkleTree.LeafManifest> manifests = Map.of(0, manifest);

            return new AntiEntropyPeer.MerkleSnapshotResponse(
                    requestShard,
                    remoteRoot,
                    requestedLeafCount,
                    manifests
            );
        };

        AtomicReference<List<Long>> pullRef = new AtomicReference<>();
        AtomicReference<List<Long>> pushRef = new AtomicReference<>();

        AntiEntropySession.RepairExecutor repairExec = (s, pullTokens, pushTokens) -> {
            pullRef.set(List.copyOf(pullTokens));
            pushRef.set(List.copyOf(pushTokens));
        };

        AntiEntropySession session = new AntiEntropySession(
                shard,
                snapshotProvider,
                peer,
                repairExec,
                leafCount
        );

        boolean inSync = session.runOnce();
        assertFalse(inSync, "Session should report out-of-sync when roots differ");

        List<Long> pull = pullRef.get();
        List<Long> push = pushRef.get();

        assertNotNull(pull, "pullTokens should be set");
        assertNotNull(push, "pushTokens should be set");

        // Local only: token 20 -> push
        // Remote only: token 30 -> pull
        assertTrue(pull.contains(30L), "Should pull token 30 from peer");
        assertFalse(pull.contains(20L), "Token 20 is local-only, should not be pulled");

        assertTrue(push.contains(20L), "Should push token 20 to peer");
        assertFalse(push.contains(30L), "Token 30 is remote-only, should not be pushed from local");

        assertTrue(pull.size() >= 1, "At least one pull token expected");
        assertTrue(push.size() >= 1, "At least one push token expected");
    }
}
