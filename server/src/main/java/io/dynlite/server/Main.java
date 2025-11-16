// file: src/main/java/io/dynlite/server/Main.java
package io.dynlite.server;

import io.dynlite.storage.*;
import io.dynlite.cluster.*;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Entry point for a single DynamoLite node.
 * Responsibilities:
 *  - Parse configuration from CLI.
 *  - Wire together storage components (WAL, snapshots, deduper, DurableStore).
 *  - Create KvService and WebServer (per node engine)
 *  - Create cluster config, ring routing, and CoordinatorService.
 *  - Start WebServer that speaks in terms of cluster operations.
 */
public final class Main {
    public static void main(String[] args) {
        var cfg = ServerConfig.fromArgs(args);

        // ------ Storage Layer -------
        var wal = new FileWal(Path.of(cfg.walDir()), 64L * 1024 * 1024); // rotate ~64MB
        var snaps = new FileSnapshotter(Path.of(cfg.snapDir()));
        var dedupe = new SimpleOpIdDeduper(); // TODO: use your real deduper; for now a small in-mem impl
        var store = new DurableStore(wal, snaps, dedupe);

        // Per-node KV Engine (vector clocks, siblings, tombstones).
        var kvService = new KvService(store, cfg.nodeId());

        // Cluster + Coordinator
        // For now, single-node cluster. Later, read this from config/CLI.
        var localNode = new ClusterConfig.Node(cfg.nodeId(), "localhost", cfg.httpPort());
        var clusterConfig = new ClusterConfig(
                cfg.nodeId(),
                List.of(localNode),
                /*N*/ 1,
                /*R*/ 1,
                /*W*/ 1,
                /*vnodes*/ 128
        );
        var routing = new RingRouting(clusterConfig);

        // NodeClient registry: only local node has a client in this process.
        NodeClient localClient = new LocalNodeClient(cfg.nodeId(), kvService);
        var clients = Map.of(cfg.nodeId(), localClient);

        var coordinator = new CoordinatorService(clusterConfig, routing, clients);

        // ----- HTTP layer -----
        var web = new WebServer(cfg.httpPort(), coordinator, kvService);
        System.out.printf(
            "Server %s listening on http://localhost:%d%n",
            cfg.nodeId(),
            cfg.httpPort()
        );
        web.start();
    }

    /**
     * Minimal in-process opId deduper.
     * For production:
     *  - replace with a TTL-bounded cache (e.g., Caffeine).
     *  - expose TTL configuration via ServerConfig.
     */
    static final class SimpleOpIdDeduper implements OpIdDeduper {
        private final java.util.Set<String> seen = java.util.concurrent.ConcurrentHashMap.newKeySet();
        @Override public boolean firstTime(String opId) { return seen.add(opId); }
        @Override public void setTtl(java.time.Duration ttl) { /* no-op */ }
    }
}
