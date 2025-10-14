package io.dynlite.server;

public record ServerConfig(
        String nodeId,         // used to bump vector clocks
        int httpPort,          // human/CLI API
        String walDir,         // local durability path
        String snapDir         // snapshot path
) {
    public static ServerConfig fromArgs(String[] args) {
        String nodeId = "node-a";
        int port = 8080;
        String wal = "./data/wal";
        String snap = "./data/snap";
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--node-id" -> nodeId = args[++i];
                case "--http-port" -> port = Integer.parseInt(args[++i]);
                case "--wal" -> wal = args[++i];
                case "--snap" -> snap = args[++i];
            }
        }
        return new ServerConfig(nodeId, port, wal, snap);
    }
}
