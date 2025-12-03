// file: server/src/main/java/io/dynlite/server/antientropy/GossipScheduler.java
package io.dynlite.server.antientropy;

import io.dynlite.server.cluster.ClusterConfig;
import io.dynlite.server.shard.ShardDescriptor;
import io.dynlite.server.shard.TokenRange;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodic gossip / anti-entropy daemon.
 *
 * Responsibilities:
 *  - On a fixed interval, pick a peer and a shard.
 *  - Construct an AntiEntropySession with:
 *      * local ShardSnapshotProvider,
 *      * HttpAntiEntropyPeer pointing at the peer's admin HTTP endpoint,
 *      * a RepairExecutor (currently logging-only; can be upgraded later).
 *  - Call runOnce() and log the outcome.
 *
 * This is intentionally simple and aimed at the demo / prototype:
 *  - We treat the local node's ownership as a single shard over the full token range.
 *  - HTTP ports are derived from node ordering: baseHttpPort + index.
 *    For the demo cluster (3 nodes: node-a, node-b, node-c), this gives 8080/8081/8082.
 */
public final class GossipScheduler {

    private final ClusterConfig cluster;
    private final String localNodeId;
    private final ShardSnapshotProvider snapshotProvider;
    private final int leafCount;
    private final int baseHttpPort;
    private final Duration period;

    private final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
    private final Random random = new Random();

    private volatile boolean started = false;

    /**
     * @param cluster          cluster configuration (nodes, localNodeId, etc.).
     * @param localNodeId      this node's id.
     * @param snapshotProvider local ShardSnapshotProvider over DurableStore.
     * @param leafCount        Merkle leaf count (e.g., 1024).
     * @param baseHttpPort     base HTTP port for index 0; peers are assumed at
     *                         baseHttpPort + indexInClusterNodes.
     * @param period           how often to run one anti-entropy round.
     */
    public GossipScheduler(ClusterConfig cluster,
                           String localNodeId,
                           ShardSnapshotProvider snapshotProvider,
                           int leafCount,
                           int baseHttpPort,
                           Duration period) {
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId");
        this.snapshotProvider = Objects.requireNonNull(snapshotProvider, "snapshotProvider");
        this.leafCount = leafCount;
        this.baseHttpPort = baseHttpPort;
        this.period = Objects.requireNonNull(period, "period");
    }

    public void start() {
        if (started) {
            return;
        }
        started = true;
        exec.scheduleAtFixedRate(this::tick, period.toMillis(), period.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void stop() {
        exec.shutdownNow();
    }

    private void tick() {
        try {
            var peers = remoteNodes();
            if (peers.isEmpty()) {
                return;
            }

            // Pick a random peer.
            var peerNode = peers.get(random.nextInt(peers.size()));

            // Build base URI for that peer's HTTP API:
            // For demo: node at index i is assumed to listen on baseHttpPort + i.
            int idx = cluster.nodes().indexOf(peerNode);
            int httpPort = baseHttpPort + idx;
            URI baseUri = URI.create("http://" + peerNode.host() + ":" + httpPort);

            var peer = new HttpAntiEntropyPeer(peerNode.nodeId(), baseUri);

            // For now: use a single shard that spans the full token space.
            TokenRange range = new TokenRange(Long.MIN_VALUE, Long.MAX_VALUE);
            ShardDescriptor shard = new ShardDescriptor(
                    "full-range-" + localNodeId,
                    localNodeId,
                    range
            );

            boolean inSync = isInSync(peerNode, shard, peer);
            if (inSync) {
                System.out.printf(
                        "[gossip] shard=%s peer=%s already in sync (matching Merkle roots)%n",
                        shard.shardId(),
                        peerNode.nodeId()
                );
            }
        } catch (Exception e) {
            System.err.println("[gossip] error during anti-entropy tick: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    private boolean isInSync(ClusterConfig.Node peerNode, ShardDescriptor shard, HttpAntiEntropyPeer peer) {
        AntiEntropySession.RepairExecutor repairExec = (s, pullTokens, pushTokens) -> {
            if (pullTokens.isEmpty() && pushTokens.isEmpty()) {
                System.out.printf(
                        "[gossip] shard=%s peer=%s already in sync%n",
                        s.shardId(), peerNode.nodeId()
                );
            } else {
                System.out.printf(
                        "[gossip] shard=%s peer=%s pullTokens=%d pushTokens=%d%n",
                        s.shardId(),
                        peerNode.nodeId(),
                        pullTokens.size(),
                        pushTokens.size()
                );
            }
            // TODO: implement real repair via NodeClient / KvService.
        };

        AntiEntropySession session =
                new AntiEntropySession(shard, snapshotProvider, peer, repairExec, leafCount);

        return session.runOnce();
    }

    private List<ClusterConfig.Node> remoteNodes() {
        List<ClusterConfig.Node> out = new ArrayList<>();
        for (ClusterConfig.Node n : cluster.nodes()) {
            if (!n.nodeId().equals(localNodeId)) {
                out.add(n);
            }
        }
        return out;
    }
}
