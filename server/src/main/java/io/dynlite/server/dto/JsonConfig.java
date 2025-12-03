package io.dynlite.server.dto;

import java.util.List;

public class JsonConfig {
    public String localNodeId;
    public int replicationFactor;
    public int readQuorum;
    public int writeQuorum;
    public int vnodes;
    public List<JsonNode> nodes;
}
