// file: server/src/main/java/io/dynlite/server/ServerConfig.java
package io.dynlite.server;

/**
 * Per-node server configuration parsed from CLI args.
 *
 * Supports:
 *  - nodeId:            logical node identity (used in vector clocks, cluster config)
 *  - httpPort:          external HTTP API port
 *  - grpcPort:          internal gRPC replica port
 *  - walDir:            directory for WAL segments
 *  - snapDir:           directory for snapshots
 *  - dedupeTtlSeconds:  TTL window for opId deduper (recent retries)
 *  - clusterConfigPath: optional JSON cluster config for multi-node setups
 */
public record ServerConfig(
        String nodeId,
        int httpPort,
        int grpcPort,
        String walDir,
        String snapDir,
        long dedupeTtlSeconds,
        String clusterConfigPath
) {

    /**
     * Very small CLI parser.
     *
     * Supported flags:
     *   --node-id,   -n   <id>
     *   --http-port, -p   <port>
     *   --grpc-port, -g   <port>
     *   --wal,       -w   <path>
     *   --snap,      -s   <path>
     *   --dedupe-ttl-seconds <seconds>
     *   --cluster-config, -c <path>
     *   --help,      -h
     *
     * All flags are optional; defaults are reasonable for local dev.
     */
    public static ServerConfig fromArgs(String[] args) {
        // Defaults
        String nodeId = "node-a";
        int httpPort = 8080;
        int grpcPort = 50051;
        String wal = "./data/wal";
        String snap = "./data/snap";
        long dedupeTtlSeconds = 600; // default: 10 minutes
        String clusterConfigPath = null;

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
                        httpPort = Integer.parseInt(args[++i]);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid http-port: " + args[i]);
                        System.exit(1);
                    }
                }

                case "--grpc-port", "-g" -> {
                    ensureValue(args, i);
                    try {
                        grpcPort = Integer.parseInt(args[++i]);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid grpc-port: " + args[i]);
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

                case "--dedupe-ttl-seconds" -> {
                    ensureValue(args, i);
                    dedupeTtlSeconds = Long.parseLong(args[++i]);
                }

                case "--cluster-config", "-c" -> {
                    ensureValue(args, i);
                    clusterConfigPath = args[++i];
                }

                default -> {
                    System.err.println("Unknown option: " + args[i]);
                    printHelpAndExit();
                }
            }
        }
        return new ServerConfig(
                nodeId,
                httpPort,
                grpcPort,
                wal,
                snap,
                dedupeTtlSeconds,
                clusterConfigPath
        );
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
              --node-id,        -n   Node identifier (default: node-a)
              --http-port,      -p   HTTP port (default: 8080)
              --grpc-port,      -g   gRPC replica port (default: 50051)
              --wal,            -w   WAL directory (default: ./data/wal)
              --snap,           -s   Snapshot directory (default: ./data/snap)
              --dedupe-ttl-seconds    TTL for opId deduper in seconds (default: 600)
              --cluster-config, -c   Path to JSON cluster config (optional)
              --help,           -h   Show this help message
            """);
        System.exit(0);
    }
}
