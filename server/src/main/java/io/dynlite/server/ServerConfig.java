// file: src/main/java/io/dynlite/server/ServerConfig.java
package io.dynlite.server;

/**
 * Immutable configuration for a single DynamoLite server process.
 * <p>
 * Today this is populated from command-line arguments only.
 * In the future, this could be extended to:
 * - read from a config file,
 * - include cluster membership, hash ring settings, etc.
 */
public record ServerConfig(
        String nodeId,   // logical identity for this node; used in vector clocks
        int httpPort, // external HTTP API port
        String walDir,   // directory for WAL segment files
        String snapDir   // directory for snapshots
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

                default -> {
                    System.err.println("Unknown option: " + args[i]);
                    printHelpAndExit();
                }
            }
        }
        return new ServerConfig(nodeId, port, wal, snap);
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


