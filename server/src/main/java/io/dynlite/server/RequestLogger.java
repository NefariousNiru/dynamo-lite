// file: server/src/main/java/io/dynlite/server/RequestLogger.java
package io.dynlite.server;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Minimal hook for request-level metrics & logging.
 *
 * Responsibilities:
 *  - Central place to log method/path/status and latency.
 *  - Can later be swapped for a metrics backend (Micrometer, Prometheus, etc.).
 */
public final class RequestLogger {
    private static final Logger log = Logger.getLogger(RequestLogger.class.getName());

    private RequestLogger() {
        // utility
    }

    /**
     * Log a completed HTTP request.
     *
     * @param method        HTTP method (GET, PUT, DELETE, etc.)
     * @param path          request path
     * @param status        HTTP status code
     * @param totalMillis   wall-clock latency for the whole request
     * @param storageMillis optional storage latency (KvService call), or -1 if not measured
     * @param error         optional exception (for 5xx logging), null if none
     */
    public static void logRequest(
            String method,
            String path,
            int status,
            long totalMillis,
            long storageMillis,
            Throwable error
    ) {
        String msg = String.format(
                "HTTP %s %s -> %d (total=%dms%s)",
                method,
                path,
                status,
                totalMillis,
                storageMillis >= 0 ? ", storage=" + storageMillis + "ms" : ""
        );

        if (error != null && status >= 500) {
            log.log(Level.WARNING, msg, error);
        } else if (status >= 500) {
            log.log(Level.WARNING, msg);
        } else {
            log.log(Level.INFO, msg);
        }
    }
}
