// file: server/src/main/java/io/dynlite/server/Main.java
package io.dynlite.server;

import io.dynlite.server.cluster.ClusterConfig;
import io.dynlite.server.cluster.CoordinatorService;
import io.dynlite.server.cluster.GrpcNodeClient;
import io.dynlite.server.cluster.LocalNodeClient;
import io.dynlite.server.cluster.NodeClient;
import io.dynlite.server.cluster.RingRouting;
import io.dynlite.server.replica.GrpcKvReplicaService;
import io.dynlite.storage.DurableStore;
import io.dynlite.storage.FileSnapshotter;
import io.dynlite.storage.FileWal;
import io.dynlite.storage.TtlOpIdDeduper;
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
 *  - Wire storage components (WAL, snapshots, deduper, DurableStore).
 *  - Create KvService and WebServer (per-node engine).
 *  - Load ClusterConfig and construct CoordinatorService.
 *  - Start HTTP server for client traffic.
 *  - Start gRPC server for replica traffic.
 */
public final class Main {

    public static void main(String[] args) {
        ServerConfig cfg = ServerConfig.fromArgs(args);

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
        Map<String, NodeClient> clients = buildNodeClients(clusterConfig, kvService, cfg.nodeId());

        var coordinator = new CoordinatorService(clusterConfig, routing, clients);

        // ------ HTTP layer ------
        var web = new WebServer(cfg.httpPort(), coordinator, kvService);

        // ------ gRPC replica server ------
        Server grpcServer = startGrpcServer(cfg, kvService);

        System.out.printf(
                "Server %s listening on http://%s:%d (HTTP) and grpc://%s:%d (replica)%n",
                cfg.nodeId(),
                "localhost", cfg.httpPort(),
                "0.0.0.0", cfg.grpcPort()
        );

        try {
            web.start();
            grpcServer.start();
        } catch (IOException e) {
            throw new RuntimeException("Failed to start servers", e);
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

        // Keep main thread alive
        try {
            grpcServer.awaitTermination();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Build ClusterConfig either from JSON (multi-node) or using a single-node fallback.
     *
     * For single-node fallback, we ensure the node's gRPC port matches cfg.grpcPort().
     */
    private static ClusterConfig buildClusterConfig(ServerConfig cfg) {
        if (cfg.clusterConfigPath() != null && !cfg.clusterConfigPath().isBlank()) {
            // Multi-node: use JSON config (includes nodes, N/R/W, vnodes).
            // Assumption: JSON already has the correct host/port for each node.
            return ClusterConfig.fromJsonFile(Path.of(cfg.clusterConfigPath()));
        }

        // Fallback: single-node cluster for local dev / tests
        ClusterConfig.Node localNode =
                new ClusterConfig.Node(cfg.nodeId(), "localhost", cfg.grpcPort());

        return new ClusterConfig(
                cfg.nodeId(),          // localNodeId
                List.of(localNode),    // nodes
                1,                     // N
                1,                     // R
                1,                     // W
                128                    // vnodes
        );
    }

    /**
     * Construct NodeClient map:
     *  - Local node uses LocalNodeClient (in-process).
     *  - Remote nodes use GrpcNodeClient(host, grpcPort).
     */
    private static Map<String, NodeClient> buildNodeClients(
            ClusterConfig cluster,
            KvService kvService,
            String localNodeId
    ) {
        Map<String, NodeClient> clients = new HashMap<>();

        for (ClusterConfig.Node n : cluster.nodes()) {
            if (n.nodeId().equals(localNodeId)) {
                clients.put(n.nodeId(), new LocalNodeClient(localNodeId, kvService));
            } else {
                clients.put(n.nodeId(), new GrpcNodeClient(n.host(), n.port()));
            }
        }
        return clients;
    }

    /**
     * Start gRPC replica server for the local node.
     * Uses ServerConfig.grpcPort() as the listen port.
     */
    private static Server startGrpcServer(ServerConfig cfg, KvService kvService) {
        return ServerBuilder
                .forPort(cfg.grpcPort())
                .addService(new GrpcKvReplicaService(kvService))
                .build();
    }
}
