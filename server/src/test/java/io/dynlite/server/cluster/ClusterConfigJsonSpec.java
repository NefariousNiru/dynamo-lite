// file: server/src/test/java/io/dynlite/server/cluster/ClusterConfigJsonSpec.java
package io.dynlite.server.cluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies static multi-node configuration can be loaded from JSON.
 */
class ClusterConfigJsonSpec {

    @TempDir
    Path tmp;

    @Test
    void loads_three_node_cluster_from_json() throws Exception {
        String json = """
                {
                  "localNodeId": "node-a",
                  "replicationFactor": 3,
                  "readQuorum": 2,
                  "writeQuorum": 2,
                  "vnodes": 128,
                  "nodes": [
                    {"nodeId": "node-a", "host": "localhost", "httpPort": 8081},
                    {"nodeId": "node-b", "host": "localhost", "httpPort": 8082},
                    {"nodeId": "node-c", "host": "localhost", "httpPort": 8083}
                  ]
                }
                """;

        Path cfgPath = tmp.resolve("cluster.json");
        Files.writeString(cfgPath, json);

        ClusterConfig cfg = ClusterConfig.fromJsonFile(cfgPath);

        assertEquals("node-a", cfg.localNodeId());
        assertEquals(3, cfg.replicationFactor());
        assertEquals(2, cfg.readQuorum());
        assertEquals(2, cfg.writeQuorum());
        assertEquals(128, cfg.vnodes());
        assertEquals(3, cfg.nodes().size());

        var nodeB = cfg.nodes().stream()
                .filter(n -> n.nodeId().equals("node-b"))
                .findFirst()
                .orElseThrow();
        assertEquals("localhost", nodeB.host());
        assertEquals(8082, nodeB.port());
    }
}
