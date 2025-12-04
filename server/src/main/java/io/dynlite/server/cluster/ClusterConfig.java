// file: server/src/main/java/io/dynlite/server/cluster/ClusterConfig.java
package io.dynlite.server.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dynlite.server.dto.JsonConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public final class ClusterConfig {

    public record Node(
            String nodeId,
            String host,
            int port
    ) {
        public Node {
            Objects.requireNonNull(nodeId, "nodeId");
            Objects.requireNonNull(host, "host");
            if (nodeId.isBlank()) throw new IllegalArgumentException("nodeId must not be blank");
            if (port <= 0 || port > 65535) throw new IllegalArgumentException("port out of range");
        }
    }

    private final String localNodeId;
    private final List<Node> nodes;
    private final int replicationFactor; // N
    private final int readQuorum;        // R
    private final int writeQuorum;       // W
    private final int vnodes;

    public ClusterConfig(
            String localNodeId,
            List<Node> nodes,
            int replicationFactor,
            int readQuorum,
            int writeQuorum,
            int vnodes
    ) {
        if (nodes == null || nodes.isEmpty()) throw new IllegalArgumentException("nodes must not be empty");
        if (replicationFactor <= 0) throw new IllegalArgumentException("replicationFactor must be > 0");
        if (replicationFactor > nodes.size()) throw new IllegalArgumentException("replicationFactor cannot exceed node count");
        if (readQuorum <= 0 || writeQuorum <= 0) throw new IllegalArgumentException("R and W must be > 0");
        if (readQuorum > replicationFactor || writeQuorum > replicationFactor) throw new IllegalArgumentException("R and W must be <= replicationFactor");
        if (vnodes <= 0) throw new IllegalArgumentException("vnodes must be > 0");

        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId");
        this.nodes = List.copyOf(nodes);
        this.replicationFactor = replicationFactor;
        this.readQuorum = readQuorum;
        this.writeQuorum = writeQuorum;
        this.vnodes = vnodes;
    }

    // New API: lets Main override localNodeId with the CLI --node-id
    public static ClusterConfig fromJsonFile(Path path, String overrideLocalNodeId) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonConfig cfg = mapper.readValue(path.toFile(), JsonConfig.class);
            List<Node> nodeList = cfg.nodes.stream()
                    .map(n -> new Node(n.nodeId, n.host, n.httpPort))
                    .toList();

            String localId = (overrideLocalNodeId != null && !overrideLocalNodeId.isBlank())
                    ? overrideLocalNodeId
                    : cfg.localNodeId;

            return new ClusterConfig(
                    localId,
                    nodeList,
                    cfg.replicationFactor,
                    cfg.readQuorum,
                    cfg.writeQuorum,
                    cfg.vnodes
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to load ClusterConfig from " + path, e);
        }
    }

    public String localNodeId() {
        return localNodeId;
    }

    public List<Node> nodes() {
        return nodes;
    }

    public int replicationFactor() {
        return replicationFactor;
    }

    public int readQuorum() {
        return readQuorum;
    }

    public int writeQuorum() {
        return writeQuorum;
    }

    public int vnodes() {
        return vnodes;
    }

    public Node localNode() {
        return nodes.stream()
                .filter(n -> n.nodeId().equals(localNodeId))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "localNodeId %s not present in cluster nodes".formatted(localNodeId)
                ));
    }
}
