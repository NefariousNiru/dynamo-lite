// file: server/src/main/java/io/dynlite/server/antientropy/GossipRepairDaemon.java
package io.dynlite.server.antientropy;

import io.dynlite.core.merkle.MerkleTree;
import io.dynlite.server.shard.ShardDescriptor;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/**
 * Periodic background daemon that drives anti-entropy for this node.
 *
 * Responsibilities:
 *  - Periodically pick a shard owned by this node.
 *  - Pick a peer node to compare with (typically one of the replicas).
 *  - Construct an {@link AntiEntropySession} and invoke {@link AntiEntropySession#runOnce()}.
 *  - Delegate actual repairs (push/pull of keys) to a caller-provided {@link AntiEntropySession.RepairExecutor}.
 *
 * This class does NOT know how to:
 *  - talk to peers (that is the job of {@link AntiEntropyPeer}, e.g. HttpAntiEntropyPeer),
 *  - read/write storage (that is the job of the RepairExecutor and snapshot provider),
 *  - plan shards (caller passes the list of shards owned by this node).
 *
 * Usage sketch:
 *
 * <pre>{@code
 *  ShardSnapshotProvider snapshotProvider = new DurableStoreShardSnapshotProvider(durableStore);
 *
 *  Map<String, AntiEntropyPeer> peers = Map.of(
 *      "node-b", new HttpAntiEntropyPeer("http://localhost:8081"),
 *      "node-c", new HttpAntiEntropyPeer("http://localhost:8082")
 *  );
 *
 *  List<ShardDescriptor> localShards = ...; // shards where ownerNodeId == localNodeId
 *
 *  AntiEntropySession.RepairExecutor repairExec = (shard, pullTokens, pushTokens) -> {
 *      // TODO: implement real repair (fetch/push keys via NodeClient or HTTP),
 *      // for now this can just log the lists.
 *  };
 *
 *  GossipRepairDaemon daemon = new GossipRepairDaemon(
 *      localNodeId,
 *      localShards,
 *      peers,
 *      snapshotProvider,
 *      repairExec,
 *      1024,                  // leafCount for Merkle trees
 *      Duration.ofSeconds(5)  // run every 5 seconds
 *  );
 *
 *  daemon.start();
 * }</pre>
 */
public final class GossipRepairDaemon {

    private final String localNodeId;
    private final List<ShardDescriptor> localShards;
    private final Map<String, AntiEntropyPeer> peerMap;
    private final ShardSnapshotProvider snapshotProvider;
    private final AntiEntropySession.RepairExecutor repairExecutor;
    private final int leafCount;
    private final long intervalMillis;
    private final Random random = new Random();

    private volatile boolean running = false;
    private Thread workerThread;

    /**
     * Create a new gossip daemon.
     *
     * @param localNodeId      nodeId for this process; used only to avoid picking ourselves as a peer.
     * @param localShards      list of shards owned by this node (ownerNodeId == localNodeId).
     * @param peerMap          mapping from peer nodeId -> AntiEntropyPeer (e.g. HttpAntiEntropyPeer).
     * @param snapshotProvider provider that can build Merkle snapshots for a shard.
     * @param repairExecutor   callback that applies repairs (pull/push tokens) to storage.
     * @param leafCount        number of leaves for the MerkleTree (should match what peers use).
     * @param interval         how often to run a single repair attempt.
     */
    public GossipRepairDaemon(
            String localNodeId,
            List<ShardDescriptor> localShards,
            Map<String, AntiEntropyPeer> peerMap,
            ShardSnapshotProvider snapshotProvider,
            AntiEntropySession.RepairExecutor repairExecutor,
            int leafCount,
            Duration interval
    ) {
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId");
        this.localShards = Objects.requireNonNull(localShards, "localShards");
        this.peerMap = Objects.requireNonNull(peerMap, "peerMap");
        this.snapshotProvider = Objects.requireNonNull(snapshotProvider, "snapshotProvider");
        this.repairExecutor = Objects.requireNonNull(repairExecutor, "repairExecutor");
        if (leafCount <= 0) throw new IllegalArgumentException("leafCount must be > 0");
        this.leafCount = leafCount;

        Objects.requireNonNull(interval, "interval");
        if (interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException("interval must be positive");
        }
        this.intervalMillis = interval.toMillis();
    }

    /**
     * Start the background gossip thread.
     * Calling start() when already running is a no-op.
     */
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        workerThread = new Thread(this::runLoop, "gossip-repair-daemon");
        workerThread.setDaemon(true);
        workerThread.start();
    }

    /**
     * Stop the background gossip thread.
     * This method returns immediately; the thread will exit after the current
     * sleep/iteration finishes.
     */
    public synchronized void stop() {
        running = false;
        if (workerThread != null) {
            workerThread.interrupt();
        }
    }

    private void runLoop() {
        while (running) {
            try {
                runOneAttempt();
            } catch (Throwable t) {
                // Swallow errors so one bad peer/shard does not kill the daemon.
                System.err.println("[gossip] Error during anti-entropy round: " + t);
                t.printStackTrace(System.err);
            }

            try {
                Thread.sleep(intervalMillis);
            } catch (InterruptedException ie) {
                // Exit promptly if stop() was called.
                if (!running) return;
            }
        }
    }

    /**
     * Run a single "gossip tick":
     *  - choose a shard owned by this node,
     *  - choose a peer,
     *  - construct and run an AntiEntropySession.
     *
     * If there are no shards or no peers, this becomes a no-op.
     */
    private void runOneAttempt() {
        if (localShards.isEmpty() || peerMap.isEmpty()) {
            return;
        }

        ShardDescriptor shard = pickRandomShard();
        AntiEntropyPeer peer = pickRandomPeer();
        if (peer == null) {
            // Only ourselves in the peerMap; nothing to do.
            return;
        }

        AntiEntropySession session = new AntiEntropySession(
                shard,
                snapshotProvider,
                peer,
                repairExecutor,
                leafCount
        );

        boolean inSync = session.runOnce();
        if (inSync) {
            // Optional: log at debug-level in real code.
            // System.out.println("[gossip] Shard " + shard + " already in sync with peer.");
        }
    }

    private ShardDescriptor pickRandomShard() {
        int idx = random.nextInt(localShards.size());
        return localShards.get(idx);
    }

    private AntiEntropyPeer pickRandomPeer() {
        if (peerMap.isEmpty()) return null;

        // Filter out localNodeId; if nothing remains, return null.
        var candidates = peerMap.entrySet().stream()
                .filter(e -> !e.getKey().equals(localNodeId))
                .toList();

        if (candidates.isEmpty()) {
            return null;
        }

        int idx = random.nextInt(candidates.size());
        return candidates.get(idx).getValue();
    }
}
