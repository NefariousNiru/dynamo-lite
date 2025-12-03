// file: server/src/main/java/io/dynlite/server/Main.java
package io.dynlite.server;

import io.dynlite.server.antientropy.AntiEntropyPeer;
import io.dynlite.server.antientropy.AntiEntropySession;
import io.dynlite.server.antientropy.DurableStoreShardSnapshotProvider;
import io.dynlite.server.antientropy.GossipRepairDaemon;
import io.dynlite.server.antientropy.HttpAntiEntropyPeer;
import io.dynlite.server.cluster.*;
import io.dynlite.server.replica.GrpcKvReplicaService;
import io.dynlite.server.shard.ShardDescriptor;
import io.dynlite.server.shard.TokenRange;
import io.dynlite.storage.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

/**
 * Entry point for a single DynamoLite node.
 * Responsibilities:
 *  - Parse configuration from CLI.
 *  - Wire together storage components (WAL, snapshots, deduper, DurableStore).
 *  - Create KvService and WebServer (per node engine)
 *  - Create cluster config, ring routing, and CoordinatorService.
 *  - Start WebServer that speaks in terms of cluster operations.
 *  - Start GRPC Server.
 *  - Start a background gossip daemon for Merkle-based anti-entropy.
 */
public final class Main {
    public static void main(String[] args) throws IOException {
        var cfg = ServerConfig.fromArgs(args);

        // ------ Storage Layer -------
        var wal = new FileWal(Path.of(cfg.walDir()), 64L * 1024 * 1024); // rotate ~64MB
        var snaps = new FileSnapshotter(Path.of(cfg.snapDir()));
        var dedupe = new TtlOpIdDeduper(Duration.ofSeconds(cfg.dedupeTtlSeconds()));
        var store = new DurableStore(wal, snaps, dedupe);

        // Per-node KV Engine (vector clocks, siblings, tombstones).
        var kvService = new KvService(store, cfg.nodeId());

        // ------ Cluster + Coordinator -------
        ClusterConfig clusterConfig = buildClusterConfig(cfg);
        var routing = new RingRouting(clusterConfig);
        Map<String, NodeClient> clients = buildNodeClients(clusterConfig, kvService);

        var coordinator = new CoordinatorService(clusterConfig, routing, clients);

        // ------ HTTP layer ------
        var web = new WebServer(cfg.httpPort(), coordinator, kvService);

        // ------ gRPC replica server ------
        Server grpcServer = startGrpcServer(clusterConfig, kvService);

        // ------ Anti-entropy / gossip wiring ------
        // 1) Snapshot provider over DurableStore.
        var snapshotProvider = new DurableStoreShardSnapshotProvider(store);

        // 2) For now: single "full-range" shard per node.
        //    TokenRange values are not used for filtering yet; the snapshot provider
        //    simply walks the whole store on both sides.
        var fullRange = new TokenRange(Long.MIN_VALUE, Long.MAX_VALUE);
        List<ShardDescriptor> localShards = List.of(
                new ShardDescriptor(cfg.nodeId() + "-full", cfg.nodeId(), fullRange)
        );

        // 3) Build AntiEntropyPeer map using HTTP endpoints.
        Map<String, AntiEntropyPeer> peerMap = buildAntiEntropyPeers(clusterConfig);

        // 4) Simple RepairExecutor: just log which tokens would be pulled/pushed.
        AntiEntropySession.RepairExecutor repairExec = (shard, pullTokens, pushTokens) -> {
            if (pullTokens.isEmpty() && pushTokens.isEmpty()) {
                return;
            }
            System.out.printf(
                    "[repair] shard=%s pullTokens=%s pushTokens=%s%n",
                    shard.shardId(),
                    pullTokens,
                    pushTokens
            );
            // TODO: later, implement real repair by fetching/pushing key versions.
        };

        // 5) Start gossip daemon if we actually have peers.
        if (!peerMap.isEmpty()) {
            GossipRepairDaemon daemon = new GossipRepairDaemon(
                    cfg.nodeId(),
                    localShards,
                    peerMap,
                    snapshotProvider,
                    repairExec,
                    /* leafCount */ 1024,
                    Duration.ofSeconds(10) // every 10s for now
            );
            daemon.start();
        }

        System.out.printf(
                "Server %s listening on http://%s:%d (HTTP) and grpc://%s:%d (replica)%n",
                cfg.nodeId(),
                "localhost", cfg.httpPort(),
                clusterConfig.localNode().host(),
                clusterConfig.localNode().port()
        );

        // Start servers
        web.start();
        try {
            grpcServer.start();
        } catch (IOException e) {
            throw new RuntimeException("Failed to start gRPC server", e);
        }

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                grpcServer.shutdown();
                web.stop();
                wal.close();
            } catch (Exception ignored) {
            }
        }));
    }

    private static ClusterConfig buildClusterConfig(ServerConfig cfg) {
        if (cfg.clusterConfigPath() != null && !cfg.clusterConfigPath().isBlank()) {
            return ClusterConfig.fromJsonFile(Path.of(cfg.clusterConfigPath()));
        }

        var localNode = new ClusterConfig.Node(cfg.nodeId(), "localhost", /*grpcPort*/ 50051);
        return new ClusterConfig(
                cfg.nodeId(),
                List.of(localNode),
                1, // N
                1, // R
                1, // W
                128 // vnodes
        );
    }

    private static Map<String, NodeClient> buildNodeClients(ClusterConfig cluster, KvService kvService) {
        Map<String, NodeClient> clients = new HashMap<>();
        String localId = cluster.localNodeId();

        for (ClusterConfig.Node n : cluster.nodes()) {
            if (n.nodeId().equals(localId)) {
                clients.put(n.nodeId(), new LocalNodeClient(localId, kvService));
            } else {
                clients.put(n.nodeId(), new GrpcNodeClient(n.host(), n.port()));
            }
        }
        return clients;
    }

    private static Server startGrpcServer(ClusterConfig cluster, KvService kvService) {
        ClusterConfig.Node local = cluster.localNode();
        int grpcPort = local.port();

        return ServerBuilder
                .forPort(grpcPort)
                .addService(new GrpcKvReplicaService(kvService))
                .build();
    }

    /**
     * Build map of nodeId -> AntiEntropyPeer using HTTP endpoints.
     *
     * For the current demo setup we derive HTTP ports from gRPC ports using:
     *   50051 -> 8080, 50052 -> 8081, 50053 -> 8082
     *
     * This matches your run-demo.sh configuration. Later, you can add explicit
     * HTTP ports to ClusterConfig to avoid this convention.
     */
    private static Map<String, AntiEntropyPeer> buildAntiEntropyPeers(ClusterConfig clusterConfig) {
        Map<String, AntiEntropyPeer> peers = new HashMap<>();

        for (ClusterConfig.Node n : clusterConfig.nodes()) {
            String nodeId = n.nodeId();
            String host = n.host();
            int grpcPort = n.port();

            // Demo-only mapping: assume ports are aligned like 50051->8080.
            int httpPort = 8080 + (grpcPort - 50051);
            String baseUrl = "http://" + host + ":" + httpPort;

            peers.put(
                    nodeId,
                    new HttpAntiEntropyPeer(nodeId, URI.create(baseUrl))
            );
        }

        return peers;
    }
}
