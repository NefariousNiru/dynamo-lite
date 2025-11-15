// file: src/main/java/io/dynlite/server/WebServer.java
package io.dynlite.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dynlite.server.dto.*;
import io.undertow.Undertow;
import io.undertow.util.Headers;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Thin HTTP adapter over KvService.
 * Responsibilities:
 *  - Parse HTTP method + path.
 *  - Decode JSON request bodies into DTOs.
 *  - Convert KvService results back into JSON.
 *  - Map Java exceptions to HTTP status codes.
 * <p>
 * Path layout (v0):
 *   - GET    /kv/{key}
 *   - PUT    /kv/{key}
 *   - DELETE /kv/{key}
 *   - GET    /admin/health
 * No auth, no versioning yet. This is intentionally small so we can
 * focus on the core behavior.
 */
public final class WebServer {
    private final Undertow server;
    private final ObjectMapper json = new ObjectMapper();

    public WebServer(int port, KvService svc) {
        this.server = Undertow.builder()
                .addHttpListener(port, "0.0.0.0")
                .setHandler(exchange -> {
                    try {
                        var path = exchange.getRequestPath();
                        var method = exchange.getRequestMethod().toString();
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");

                        if (path.startsWith("/kv/")) {
                            String key = path.substring("/kv/".length());
                            switch (method) {
                                case "GET" -> handleGet(exchange, svc, key);
                                case "PUT" -> handlePut(exchange, svc, key);
                                case "DELETE" -> handleDelete(exchange, svc, key);
                                default -> send(exchange, 405, Map.of("error", "method not allowed"));
                            }
                        } else if ("/admin/health".equals(path)) {
                            send(exchange, 200, Map.of("status", "ok"));
                        } else {
                            send(exchange, 404, Map.of("error", "not found"));
                        }
                    } catch (IllegalArgumentException bad) {
                        send(exchange, 400, Map.of("error", bad.getMessage()));
                    } catch (Exception ex) {
                        send(exchange, 500, Map.of("error", ex.getClass().getSimpleName(), "message", ex.getMessage()));
                    }
                }).build();
    }

    public void start() {
        server.start();
    }

    // ---------- handlers ----------

    /** GET/kv/{key} */
    private void handleGet(io.undertow.server.HttpServerExchange ex, KvService svc, String key) throws Exception {
        var r = svc.get(key);
        if (!r.found()) {
            send(ex, 404, Map.of("found", false));
            return;
        }
        var dto = new GetResponse();
        dto.found = true;
        dto.valueBase64 = r.base64();
        dto.vectorClock = r.clock();
        send(ex, 200, dto);
    }

    /** PUT /kv/{key} */
    private void handlePut(io.undertow.server.HttpServerExchange ex, KvService svc, String key) {
        ex.getRequestReceiver().receiveFullBytes(
                (exchange, data) -> {
                    try {
                        var req = json.readValue(data, PutRequest.class);
                        var r = svc.put(key, req.valueBase64, req.nodeId);
                        var dto = new PutResponse();
                        dto.ok = true;
                        dto.tombstone = false;
                        dto.lwwMillis = r.lwwMillis();
                        dto.vectorClock = r.clock();
                        send(exchange, 200, dto);
                    } catch (IllegalArgumentException bad) {
                        send(exchange, 400, Map.of("error", bad.getMessage()));
                    } catch (Exception e) {
                        send(exchange, 500, Map.of("error", e.getClass().getSimpleName(), "message", e.getMessage()));
                    }
                },
                (exchange, ioEx) -> send(exchange, 400, Map.of("error", "invalid request body"))
        );
    }

    /** DELETE /kv/{key} */
    private void handleDelete(io.undertow.server.HttpServerExchange ex, KvService svc, String key) {
        ex.getRequestReceiver().receiveFullBytes(
                (exchange, data) -> {
                    try {
                        var req = json.readValue(data, DeleteRequest.class);
                        var r = svc.delete(key, req.nodeId);
                        var dto = new PutResponse();
                        dto.ok = true;
                        dto.tombstone = true;
                        dto.lwwMillis = r.lwwMillis();
                        dto.vectorClock = r.clock();
                        send(exchange, 200, dto);
                    } catch (IllegalArgumentException bad) {
                        send(exchange, 400, Map.of("error", bad.getMessage()));
                    } catch (Exception e) {
                        send(exchange, 500, Map.of("error", e.getClass().getSimpleName(), "message", e.getMessage()));
                    }
                },
                (exchange, ioEx) -> send(exchange, 400, Map.of("error", "invalid request body"))
        );
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
