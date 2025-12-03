// file: server/src/main/java/io/dynlite/server/Main.java
package io.dynlite.server;

import io.dynlite.server.antientropy.DurableStoreShardSnapshotProvider;
import io.dynlite.server.antientropy.GossipScheduler;
import io.dynlite.server.antientropy.ShardSnapshotProvider;
import io.dynlite.server.cluster.*;
import io.dynlite.server.replica.GrpcKvReplicaService;
import io.dynlite.storage.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.nio.file.Path;
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
 *  - Build cluster config, ring routing, and CoordinatorService.
 *  - Start HTTP + gRPC servers.
 *  - (New) Start a periodic anti-entropy gossip scheduler.
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

        // Anti-entropy: snapshot provider over DurableStore.
        ShardSnapshotProvider snapshotProvider = new DurableStoreShardSnapshotProvider(store);

        // ------ Cluster + Coordinator -------
        ClusterConfig clusterConfig = buildClusterConfig(cfg);
        var routing = new RingRouting(clusterConfig);
        Map<String, NodeClient> clients = buildNodeClients(clusterConfig, kvService);

        var coordinator = new CoordinatorService(clusterConfig, routing, clients);

        // ------ HTTP layer ------
        // Use the ctor that accepts a snapshotProvider so /admin/anti-entropy/merkle-snapshot works.
        var web = new WebServer(cfg.httpPort(), coordinator, kvService, snapshotProvider);

        // ------ gRPC replica server ------
        Server grpcServer = startGrpcServer(clusterConfig, kvService);

        // ------ gossip / anti-entropy daemon ------
        // For demo: leafCount = 1024, baseHttpPort = 8080, period = 30 seconds.
        var gossip = new GossipScheduler(
                clusterConfig,
                cfg.nodeId(),
                snapshotProvider,
                /*leafCount*/ 1024,
                /*baseHttpPort*/ 8080,
                Duration.ofSeconds(30)
        );

        System.out.printf(
                "Server %s listening on http://%s:%d (HTTP) and grpc://%s:%d (replica)%n",
                cfg.nodeId(),
                "localhost", cfg.httpPort(),
                clusterConfig.localNode().host(),
                clusterConfig.localNode().port()
        );

        // Start servers and gossip.
        web.start();
        try {
            grpcServer.start();
        } catch (IOException e) {
            throw new RuntimeException("Failed to start gRPC server", e);
        }
        gossip.start();

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

    /**
     * Build ClusterConfig either from JSON (multi-node) or using a single-node fallback.
     */
    private static ClusterConfig buildClusterConfig(ServerConfig cfg) {
        if (cfg.clusterConfigPath() != null && !cfg.clusterConfigPath().isBlank()) {
            // Multi-node: use JSON config (includes nodes, N/R/W, vnodes)
            return ClusterConfig.fromJsonFile(Path.of(cfg.clusterConfigPath()));
        }

        // Fallback: single-node cluster for local dev / tests
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

    /**
     * Construct NodeClient map:
     *  - Local node uses LocalNodeClient (in-process).
     *  - Remote nodes use GrpcNodeClient(host, grpcPort).
     */
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

    /**
     * Start gRPC replica server for the local node.
     * Uses ClusterConfig.localNode().port() as the gRPC port.
     */
    private static Server startGrpcServer(ClusterConfig cluster, KvService kvService) {
        ClusterConfig.Node local = cluster.localNode();
        int grpcPort = local.port();

        return ServerBuilder
                .forPort(grpcPort)
                .addService(new GrpcKvReplicaService(kvService))
                .build();
    }
}
