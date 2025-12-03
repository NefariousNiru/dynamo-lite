package io.dynlite.server.antientropy;

import io.dynlite.server.cluster.ClusterConfig;
import io.dynlite.server.shard.ShardDescriptor;
import io.dynlite.server.shard.TokenRange;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Very simple gossip daemon that periodically runs anti-entropy sessions
 * against a randomly chosen peer over a single "full-range" shard.
 *
 * Goals (for the plain, non-RAAE version):
 *  - Run AntiEntropySession.runOnce() in the background.
 *  - Avoid overloading the node: at most one session at a time, fixed delay.
 *  - Log basic metrics: in-sync vs out-of-sync, number of tokens scheduled
 *    for pull/push repairs.
 *
 * This is intentionally minimal and meant for:
 *  - demos,
 *  - correctness checks,
 *  - as a foundation for RAAE and more advanced scheduling later.
 */
public final class GossipDaemon {

    private final ClusterConfig cluster;
    private final String localNodeId;
    private final ShardSnapshotProvider snapshotProvider;
    private final Map<String, AntiEntropyPeer> peers;
    private final int leafCount;
    private final Duration interval;
    private final ScheduledExecutorService scheduler;
    private final Random random = new Random();

    public GossipDaemon(
            ClusterConfig cluster,
            ShardSnapshotProvider snapshotProvider,
            Map<String, AntiEntropyPeer> peers,
            int leafCount,
            Duration interval
    ) {
        this.cluster = cluster;
        this.localNodeId = cluster.localNodeId();
        this.snapshotProvider = snapshotProvider;
        this.peers = peers;
        this.leafCount = leafCount;
        this.interval = interval;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "gossip-daemon");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Start periodic gossip. At most one session runs at a time because we
     * use a single-threaded scheduler and each tick performs at most one
     * AntiEntropySession.runOnce() call.
     */
    public void start() {
        // No initial delay; adjust if you want to wait for the node to warm up.
        scheduler.scheduleWithFixedDelay(
                this::tickSafe,
                5L,
                interval.toSeconds(),
                TimeUnit.SECONDS
        );
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    // ---------- internals ----------

    private void tickSafe() {
        try {
            tick();
        } catch (Exception e) {
            System.err.println("[gossip] tick failed: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    private void tick() {
        List<ClusterConfig.Node> nodes = cluster.nodes();
        if (nodes.size() <= 1) {
            // Nothing to gossip with in single-node setups.
            return;
        }

        // Pick a random peer ≠ local.
        ClusterConfig.Node peerNode = pickRandomPeer(nodes);
        if (peerNode == null) {
            return;
        }

        String peerId = peerNode.nodeId();
        AntiEntropyPeer peer = peers.get(peerId);
        if (peer == null) {
            System.err.println("[gossip] no AntiEntropyPeer for " + peerId);
            return;
        }

        // For now, treat the entire token space as a single shard.
        TokenRange fullRange = new TokenRange(Long.MIN_VALUE, Long.MAX_VALUE);
        ShardDescriptor shard = new ShardDescriptor(
                "full-range-" + localNodeId + "->" + peerId,
                localNodeId,
                fullRange
        );

        AntiEntropySession.RepairExecutor repairExec =
                (s, pullTokens, pushTokens) -> {
                    int pulls = pullTokens.size();
                    int pushes = pushTokens.size();
                    if (pulls == 0 && pushes == 0) {
                        return;
                    }
                    System.out.printf(
                            "[gossip] shard=%s peer=%s pullTokens=%d pushTokens=%d%n",
                            s.shardId(),
                            peerId,
                            pulls,
                            pushes
                    );
                    // NOTE: This is a "plain" AE implementation: we only log which
                    // tokens differ. Actual key-level repairs will be introduced
                    // as part of the RAAE / repair-aware phase.
                };

        AntiEntropySession session = new AntiEntropySession(
                shard,
                snapshotProvider,
                peer,
                repairExec,
                leafCount
        );

        boolean inSync = session.runOnce();
        if (inSync) {
            System.out.printf(
                    "[gossip] shard=%s peer=%s status=IN_SYNC%n",
                    shard.shardId(),
                    peerId
            );
        } else {
            System.out.printf(
                    "[gossip] shard=%s peer=%s status=OUT_OF_SYNC%n",
                    shard.shardId(),
                    peerId
            );
        }
    }

    private ClusterConfig.Node pickRandomPeer(List<ClusterConfig.Node> nodes) {
        ClusterConfig.Node local = cluster.localNode();
        if (nodes.size() <= 1) {
            return null;
        }

        for (int attempt = 0; attempt < 5; attempt++) {
            ClusterConfig.Node candidate = nodes.get(random.nextInt(nodes.size()));
            if (!candidate.nodeId().equals(local.nodeId())) {
                return candidate;
            }
        }

        // Fallback: first non-local node
        for (ClusterConfig.Node n : nodes) {
            if (!n.nodeId().equals(local.nodeId())) {
                return n;
            }
        }
        return null;
    }
}
