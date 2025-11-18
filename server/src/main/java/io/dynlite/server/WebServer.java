// file: src/main/java/io/dynlite/server/WebServer.java
package io.dynlite.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dynlite.server.cluster.CoordinatorService;
import io.dynlite.server.dto.*;
import io.dynlite.server.RequestLogger;
import io.undertow.Undertow;
import io.undertow.util.Headers;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;

/**
 * Thin HTTP adapter over CoordinatorService + KvService.
 *
 * Responsibilities:
 *  - Parse HTTP method + path.
 *  - Decode JSON request bodies into DTOs.
 *  - Convert service results back into JSON.
 *  - Map Java exceptions to HTTP status codes.
 *  - Emit basic per-request logging/metrics.
 *
 * Path layout (v0):
 *   - GET    /kv/{key}              Uses Coordinator
 *   - PUT    /kv/{key}              Uses Coordinator
 *   - DELETE /kv/{key}              Uses Coordinator
 *   - GET    /debug/siblings/{key}  Local-node sibling view via KvService
 *   - GET    /admin/health          Basic health check
 *
 * No auth, no versioning yet.
 */
public final class WebServer {
    private static final int MAX_BODY_BYTES = 10 * 1024 * 1024; // 10 MiB

    private final Undertow server;
    private final ObjectMapper json = new ObjectMapper();
    private final CoordinatorService coord;
    private final KvService kv;

    public WebServer(int port, CoordinatorService coord, KvService kv) {
        this.coord = coord;
        this.kv = kv;

        this.server = Undertow.builder()
                .addHttpListener(port, "0.0.0.0")
                .setHandler(exchange -> {
                    var path = exchange.getRequestPath();
                    var method = exchange.getRequestMethod().toString();
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");

                    if (path.startsWith("/kv/")) {
                        String key = path.substring("/kv/".length());
                        if (key.isBlank()) {
                            send(exchange, 400, Map.of("error", "key must not be empty"));
                            RequestLogger.logRequest(method, path, 400, 0, -1, null);
                            return;
                        }
                        switch (method) {
                            case "PUT" -> handlePut(exchange, key);
                            case "GET" -> handleGet(exchange, key);
                            case "DELETE" -> handleDelete(exchange, key);
                            default -> {
                                send(exchange, 405, Map.of("error", "method not allowed"));
                                RequestLogger.logRequest(method, path, 405, 0, -1, null);
                            }
                        }
                    } else if (path.startsWith("/debug/siblings/") && "GET".equals(method)) {
                        String key = path.substring("/debug/siblings/".length());
                        if (key.isBlank()) {
                            send(exchange, 400, Map.of("error", "key must not be empty"));
                            RequestLogger.logRequest(method, path, 400, 0, -1, null);
                        } else {
                            handleDebugSiblings(exchange, key);
                        }
                    } else if ("/admin/health".equals(path)) {
                        send(exchange, 200, Map.of("status", "ok"));
                        RequestLogger.logRequest(method, path, 200, 0, -1, null);
                    } else {
                        send(exchange, 404, Map.of("error", "not found"));
                        RequestLogger.logRequest(method, path, 404, 0, -1, null);
                    }
                }).build();
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(); // For tests to stop server
    }

    // ---------- handlers ----------

    /** GET /kv/{key} */
    private void handleGet(io.undertow.server.HttpServerExchange ex, String key) {
        long start = System.nanoTime();
        int status = 200;
        long storageMs = -1L;
        Throwable error = null;
        try {
            long sStart = System.nanoTime();
            CoordinatorService.Read r = coord.get(key);
            storageMs = (System.nanoTime() - sStart) / 1_000_000L;

            if (!r.found()) {
                status = 404;
                send(ex, status, Map.of("found", false));
            } else {
                var dto = new GetResponse();
                dto.found = true;
                dto.valueBase64 = r.base64();
                dto.vectorClock = r.clock();
                status = 200;
                send(ex, status, dto);
            }
        } catch (IllegalArgumentException bad) {
            status = 400;
            error = bad;
            send(ex, status, Map.of("error", bad.getMessage()));
        } catch (Exception e) {
            status = 500;
            error = e;
            send(ex, status, Map.of("error", e.getClass().getSimpleName(), "message", e.getMessage()));
        } finally {
            long totalMs = (System.nanoTime() - start) / 1_000_000L;
            RequestLogger.logRequest("GET", ex.getRequestPath(), status, totalMs, storageMs, error);
        }
    }

    /** PUT /kv/{key} */
    private void handlePut(io.undertow.server.HttpServerExchange ex, String key) {
        ex.getRequestReceiver().receiveFullBytes(
                (exchange, data) -> {
                    String method = "PUT";
                    String path = exchange.getRequestPath();
                    long start = System.nanoTime();
                    int status;
                    long storageMs = -1L;
                    Throwable error = null;

                    try {
                        if (data.length > MAX_BODY_BYTES) {
                            status = 413;
                            send(exchange, status, Map.of("error", "request body too large"));
                        } else {
                            var req = json.readValue(data, PutRequest.class);
                            long sStart = System.nanoTime();
                            CoordinatorService.Result r = coord.put(key, req.valueBase64, req.nodeId, req.opId);
                            storageMs = (System.nanoTime() - sStart) / 1_000_000L;

                            var dto = new PutResponse();
                            dto.ok = true;
                            dto.tombstone = false;
                            dto.lwwMillis = r.lwwMillis();
                            dto.vectorClock = r.clock();
                            status = 200;
                            send(exchange, status, dto);
                        }
                    } catch (IllegalArgumentException bad) {
                        status = 400;
                        error = bad;
                        send(exchange, status, Map.of("error", bad.getMessage()));
                    } catch (JsonProcessingException jsonEx) {
                        status = 400;
                        error = jsonEx;
                        send(exchange, status, Map.of("error", "invalid JSON"));
                    } catch (Exception e) {
                        status = 500;
                        error = e;
                        send(exchange, status, Map.of("error", e.getClass().getSimpleName(), "message", e.getMessage()));
                    } finally {
                        long totalMs = (System.nanoTime() - start) / 1_000_000L;
                        RequestLogger.logRequest(method, path, exchange.getStatusCode(), totalMs, storageMs, error);
                    }
                },
                (exchange, ioEx) -> {
                    String method = "PUT";
                    String path = exchange.getRequestPath();
                    int status = 400;
                    send(exchange, status, Map.of("error", "invalid request body"));
                    RequestLogger.logRequest(method, path, status, 0, -1, ioEx);
                }
        );
    }

    /** DELETE /kv/{key} */
    private void handleDelete(io.undertow.server.HttpServerExchange ex, String key) {
        ex.getRequestReceiver().receiveFullBytes(
                (exchange, data) -> {
                    String method = "DELETE";
                    String path = exchange.getRequestPath();
                    long start = System.nanoTime();
                    int status;
                    long storageMs = -1L;
                    Throwable error = null;

                    try {
                        if (data.length > MAX_BODY_BYTES) {
                            status = 413;
                            send(exchange, status, Map.of("error", "request body too large"));
                        } else {
                            var req = json.readValue(data, DeleteRequest.class);
                            long sStart = System.nanoTime();
                            CoordinatorService.Result r = coord.delete(key, req.nodeId, req.opId);
                            storageMs = (System.nanoTime() - sStart) / 1_000_000L;

                            var dto = new PutResponse();
                            dto.ok = true;
                            dto.tombstone = true;
                            dto.lwwMillis = r.lwwMillis();
                            dto.vectorClock = r.clock();
                            status = 200;
                            send(exchange, status, dto);
                        }
                    } catch (IllegalArgumentException bad) {
                        status = 400;
                        error = bad;
                        send(exchange, status, Map.of("error", bad.getMessage()));
                    } catch (JsonProcessingException jsonEx) {
                        status = 400;
                        error = jsonEx;
                        send(exchange, status, Map.of("error", "invalid JSON"));
                    } catch (Exception e) {
                        status = 500;
                        error = e;
                        send(exchange, status, Map.of("error", e.getClass().getSimpleName(), "message", e.getMessage()));
                    } finally {
                        long totalMs = (System.nanoTime() - start) / 1_000_000L;
                        RequestLogger.logRequest(method, path, exchange.getStatusCode(), totalMs, storageMs, error);
                    }
                },
                (exchange, ioEx) -> {
                    String method = "DELETE";
                    String path = exchange.getRequestPath();
                    int status = 400;
                    send(exchange, status, Map.of("error", "invalid request body"));
                    RequestLogger.logRequest(method, path, status, 0, -1, ioEx);
                }
        );
    }

    /**
     * GET /debug/siblings/{key}
     * Returns the full sibling set for a key: all maximal versions under the
     * vector-clock partial order, including tombstones. Uses local {@link KvService}.
     */
    private void handleDebugSiblings(io.undertow.server.HttpServerExchange ex, String key) {
        long start = System.nanoTime();
        int status = 200;
        long storageMs = -1L;
        Throwable error = null;

        try {
            long sStart = System.nanoTime();
            var sibs = this.kv.siblings(key);
            storageMs = (System.nanoTime() - sStart) / 1_000_000L;

            SiblingDebugResponse dto = new SiblingDebugResponse();
            dto.key = key;
            dto.siblings = new ArrayList<>(sibs.size());

            for (KvService.Sibling s : sibs) {
                SiblingDebugResponse.SiblingRecord rec = new SiblingDebugResponse.SiblingRecord();
                rec.tombstone = s.tombstone();
                rec.lwwMillis = s.lwwMillis();
                rec.valueBase64 = s.valueBase64();
                rec.vectorClock = s.clock();
                dto.siblings.add(rec);
            }

            // Always 200: if there are no siblings, the list is empty.
            send(ex, status, dto);
        } catch (Exception e) {
            status = 500;
            error = e;
            send(ex, status, Map.of("error", e.getClass().getSimpleName(), "message", e.getMessage()));
        } finally {
            long totalMs = (System.nanoTime() - start) / 1_000_000L;
            RequestLogger.logRequest("GET", ex.getRequestPath(), status, totalMs, storageMs, error);
        }
    }

    /**
     * Serialize 'body' as JSON and write it with the given HTTP status code.
     */
    private void send(io.undertow.server.HttpServerExchange ex, int code, Object body) {
        try {
            ex.setStatusCode(code);
            byte[] bytes = json.writeValueAsBytes(body);
            ex.getResponseSender().send(new String(bytes, StandardCharsets.UTF_8));
        } catch (Exception e) {
            ex.setStatusCode(500);
            ex.getResponseSender().send("{\"error\":\"serialization\"}");
        }
    }
}
