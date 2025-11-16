// file: src/main/java/io/dynlite/server/ServerConfig.java
package io.dynlite.server;

/**
 * Per-node server configuration parsed from CLI args.
 * Currently supports:
 *  - nodeId:   logical node identity (used in vector clocks, cluster config)
 *  - httpPort: external HTTP API port
 *  - walDir:   directory for WAL segments
 *  - snapDir:  directory for snapshots
 *  - dedupeTtlSeconds: TTL window for opId deduper (recent retries)
 */
public record ServerConfig(
        String nodeId,   // logical identity for this node; used in vector clocks
        int httpPort, // external HTTP API port
        String walDir,   // directory for WAL segment files
        String snapDir,   // directory for snapshots
        long dedupeTtlSeconds
) {

    /**
     * Very small CLI parser.
     * Supported flags:
     * --node-id   <id>
     * --http-port <port>
     * --wal       <path>
     * --snap      <path>
     * All flags are optional; defaults are reasonable for local dev.
     */
    public static ServerConfig fromArgs(String[] args) {
        // Start Node
        String nodeId = "node-a";

        // Server Port
        int port = 8080;

        // Datastore
        String wal = "./data/wal";
        String snap = "./data/snap";

        long dedupeTtlSeconds = 600; // default: 10 minutes

        // CLIArg Parser
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--help", "-h" -> printHelpAndExit();

                case "--node-id", "-n" -> {
                    ensureValue(args, i);
                    nodeId = args[++i];
                }

                case "--http-port", "-p" -> {
                    ensureValue(args, i);
                    try {
                        port = Integer.parseInt(args[++i]);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid port: " + args[i]);
                        System.exit(1);
                    }
                }

                case "--wal", "-w" -> {
                    ensureValue(args, i);
                    wal = args[++i];
                }

                case "--snap", "-s" -> {
                    ensureValue(args, i);
                    snap = args[++i];
                }

                case "--dedupe-ttl-seconds" -> dedupeTtlSeconds = Long.parseLong(args[++i]);

                default -> {
                    System.err.println("Unknown option: " + args[i]);
                    printHelpAndExit();
                }
            }
        }
        return new ServerConfig(nodeId, port, wal, snap, dedupeTtlSeconds);
    }

    private static void ensureValue(String[] args, int i) {
        if (i + 1 >= args.length) {
            System.err.println("Missing value for option: " + args[i]);
            System.exit(1);
        }
    }

    private static void printHelpAndExit() {
        System.out.println("""
            Usage: server [options]
            
            Options:
              --node-id,   -n   Node identifier (default: node-a)
              --http-port, -p   HTTP port (default: 8080)
              --wal,       -w   WAL directory (default: ./data/wal)
              --snap,      -s   Snapshot directory (default: ./data/snap)
              --help,      -h   Show this help message
            """);
        System.exit(0);
    }
}


