package io.dynlite.server;

import io.dynlite.server.antientropy.AntiEntropyPeer;
import io.dynlite.server.antientropy.DurableStoreShardSnapshotProvider;
import io.dynlite.server.antientropy.GossipDaemon;
import io.dynlite.server.antientropy.HttpAntiEntropyPeer;
import io.dynlite.server.cluster.*;
import io.dynlite.server.replica.GrpcKvReplicaService;
import io.dynlite.storage.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.net.URI;
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
 *  - Build ClusterConfig, routing, NodeClients, and CoordinatorService.
 *  - Start HTTP server for client API.
 *  - Start gRPC server for replica traffic.
 *  - Start background gossip daemon for Merkle-based anti-entropy.
 */
public final class Main {
    public static void main(String[] args) throws IOException {
        var cfg = ServerConfig.fromArgs(args);

        // ------ Storage Layer -------
        var wal = new FileWal(Path.of(cfg.walDir()), 64L * 1024 * 1024); // rotate ~64MB
        var snaps = new FileSnapshotter(Path.of(cfg.snapDir()));
        var dedupe = new TtlOpIdDeduper(java.time.Duration.ofSeconds(cfg.dedupeTtlSeconds()));
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

        System.out.printf(
                "Server %s listening on http://%s:%d (HTTP) and grpc://%s:%d (replica)%n",
                cfg.nodeId(),
                "localhost", cfg.httpPort(),
                clusterConfig.localNode().host(),
                clusterConfig.localNode().port()
        );

        // ------ Anti-entropy gossip (plain Merkle-based) ------
        var snapshotProvider = new DurableStoreShardSnapshotProvider(store);
        Map<String, AntiEntropyPeer> aePeers = buildAntiEntropyPeers(clusterConfig);

        // Leaf count and interval are demo-friendly knobs; tweak if needed.
        int leafCount = 1024;
        Duration gossipInterval = Duration.ofSeconds(10);

        var gossip = new GossipDaemon(
                clusterConfig,
                snapshotProvider,
                aePeers,
                leafCount,
                gossipInterval
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

    /**
     * Build AntiEntropyPeer clients for all nodes in the cluster.
     *
     * We derive HTTP ports from the gRPC ports using the demo convention:
     *   gRPC: 50051 -> HTTP: 8080
     *   gRPC: 50052 -> HTTP: 8081
     *   gRPC: 50053 -> HTTP: 8082
     *
     * This matches the run-demo.sh layout and keeps configuration minimal.
     */
    private static Map<String, AntiEntropyPeer> buildAntiEntropyPeers(ClusterConfig clusterConfig) {
        Map<String, AntiEntropyPeer> peers = new HashMap<>();

        for (ClusterConfig.Node n : clusterConfig.nodes()) {
            String nodeId = n.nodeId();
            String host = n.host();
            int grpcPort = n.port();

            // Demo-only mapping: assume ports are aligned like 50051->8080.
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
