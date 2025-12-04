// file: server/src/main/java/io/dynlite/server/Main.java
package io.dynlite.server;

import io.dynlite.server.antientropy.*;
import io.dynlite.server.cluster.*;
import io.dynlite.server.replica.GrpcKvReplicaService;
import io.dynlite.server.slo.ReplicaLatencyTracker;
import io.dynlite.server.slo.SloMetrics;
import io.dynlite.server.slo.StalenessBudgetTracker;
import io.dynlite.storage.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Entry point for a single DynamoLite node.
 *
 * Responsibilities:
 *  - Parse configuration from CLI.
 *  - Wire together storage components (WAL, snapshots, deduper, DurableStore).
 *  - Create KvService and WebServer (per node engine).
 *  - Build ClusterConfig, routing, NodeClients, and CoordinatorService.
 *  - Start HTTP server for client API.
 *  - Start gRPC server for replica traffic.
 *  - Start background gossip daemon for Merkle-based anti-entropy (FIFO/RAAE).
 *  - Wire SLO-Driven Adaptive Consistency (SAC) for dynamic N/R/W decisions.
 */
public final class Main {

    private Main() {
        // no-op
    }

    // file: server/src/main/java/io/dynlite/server/Main.java
    public static void main(String[] args) throws IOException {
        var cfg = ServerConfig.fromArgs(args);

        // ------ Storage Layer -------
        var wal = new FileWal(Path.of(cfg.walDir()), 64L * 1024 * 1024);
        var snaps = new FileSnapshotter(Path.of(cfg.snapDir()));
        var dedupe = new TtlOpIdDeduper(java.time.Duration.ofSeconds(cfg.dedupeTtlSeconds()));
        var store = new DurableStore(wal, snaps, dedupe);

        // ------ RAAE tracking primitives ------
        var hotnessTracker = new RaaeHotnessTracker(0.2);
        var divergenceTracker = new RaaeDivergenceTracker();
        var scorer = new RaaeScorer(hotnessTracker, divergenceTracker);
        AntiEntropyRateLimiter rateLimiter =
                new TokenBucketRateLimiter(128L, 64.0);
        var aeMetrics = new AntiEntropyMetrics();
        var priorityScheduler = new RaaePriorityScheduler(1024);

        AntiEntropySession.RepairExecutor repairExec =
                RaaeAwareRepairExecutor.raaePriority(
                        128,
                        hotnessTracker,
                        divergenceTracker,
                        scorer,
                        rateLimiter,
                        aeMetrics,
                        priorityScheduler,
                        Clock.systemUTC()
                );

        // Per-node KV Engine
        var kvService = new KvService(store, cfg.nodeId(), hotnessTracker);

        // ------ Cluster + routing -------
        ClusterConfig clusterConfig = buildClusterConfig(cfg);
        var routing = new RingRouting(clusterConfig);

        // ------ SLO / SAC primitives ------
        var latencyTracker = new ReplicaLatencyTracker(0.3, 256);
        var stalenessBudget = new StalenessBudgetTracker(1024);
        var sloMetrics = new SloMetrics();

        Map<String, NodeClient> clients = buildNodeClients(clusterConfig, kvService);

        var coordinator = new CoordinatorService(
                clusterConfig,
                routing,
                clients,
                latencyTracker,
                stalenessBudget,
                sloMetrics
        );

        // ------ Anti-entropy snapshot provider (shared by HTTP + gossip) ------
        var snapshotProvider = new DurableStoreShardSnapshotProvider(store);

        // ------ HTTP layer + auth ------
        String authToken = System.getenv("DYNLITE_AUTH_TOKEN");
        var web = new WebServer(cfg.httpPort(), coordinator, kvService, snapshotProvider, authToken);

        // ------ gRPC replica server ------
        Server grpcServer = startGrpcServer(clusterConfig, kvService);

        System.out.printf(
                "Server %s listening on http://%s:%d (HTTP) and grpc://%s:%d (replica)%n",
                cfg.nodeId(),
                "localhost", cfg.httpPort(),
                clusterConfig.localNode().host(),
                clusterConfig.localNode().port()
        );

        if (authToken != null && !authToken.isBlank()) {
            System.out.println("HTTP bearer auth enabled (DYNLITE_AUTH_TOKEN is set).");
        } else {
            System.out.println("HTTP bearer auth disabled (no DYNLITE_AUTH_TOKEN).");
        }

        // ------ Anti-entropy gossip (FIFO/RAAE) ------
        Map<String, AntiEntropyPeer> aePeers = buildAntiEntropyPeers(clusterConfig);

        int leafCount = 1024;
        Duration gossipInterval = Duration.ofSeconds(10);

        var gossip = new GossipDaemon(
                clusterConfig,
                snapshotProvider,
                aePeers,
                leafCount,
                gossipInterval,
                repairExec
        );
        gossip.start();

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
                gossip.stop();
                grpcServer.shutdown();
                web.stop();
                wal.close();
            } catch (Exception ignored) {
            }
        }));
    }


    private static ClusterConfig buildClusterConfig(ServerConfig cfg) {
        if (cfg.clusterConfigPath() != null && !cfg.clusterConfigPath().isBlank()) {
            return ClusterConfig.fromJsonFile(Path.of(cfg.clusterConfigPath()), cfg.nodeId());
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

    private static Map<String, AntiEntropyPeer> buildAntiEntropyPeers(ClusterConfig clusterConfig) {
        Map<String, AntiEntropyPeer> peers = new HashMap<>();

        for (ClusterConfig.Node n : clusterConfig.nodes()) {
            String nodeId = n.nodeId();
            String host = n.host();
            int grpcPort = n.port();

            int httpPort = 8080 + (grpcPort - 50051);
            URI baseUri = URI.create("http://" + host + ":" + httpPort);

            peers.put(nodeId, new HttpAntiEntropyPeer(nodeId, baseUri));
        }

        return peers;
    }

    private static Server startGrpcServer(ClusterConfig cluster, KvService kvService) {
        ClusterConfig.Node local = cluster.localNode();
        int grpcPort = local.port();

        return ServerBuilder
                .forPort(grpcPort)
                .addService(new GrpcKvReplicaService(kvService))
                .build();
    }
}
