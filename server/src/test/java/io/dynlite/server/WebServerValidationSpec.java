// file: server/src/test/java/io/dynlite/server/WebServerValidationSpec.java
package io.dynlite.server;

import io.dynlite.core.VersionedValue;
import io.dynlite.server.cluster.ClusterConfig;
import io.dynlite.server.cluster.CoordinatorService;
import io.dynlite.server.cluster.NodeClient;
import io.dynlite.server.cluster.RingRouting;
import io.dynlite.storage.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end specs for WebServer validation and error semantics.
 *
 * Focus:
 *  - Empty key -> 400 "key must not be empty".
 *  - Invalid JSON -> 400 "invalid JSON".
 *  - Too-large body -> 413 "request body too large".
 *  - IllegalArgumentException from Coordinator -> 400.
 */
class WebServerValidationSpec {

    private static final int PORT = 18080; // test-only port
    private WebServer server;
    private HttpClient client;

    @BeforeEach
    void startServer() {
        // ---- 1) Minimal KeyValueStore for KvService (/debug/siblings not exercised here) ----
        KvService kv = getKvService();

        // ---- 2) Single-node ClusterConfig and RingRouting ----
        CoordinatorService coord = getCoordinatorService();

        // ---- 4) WebServer + HTTP client ----
        server = new WebServer(PORT, coord, kv);
        server.start();

        client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();
    }

    private static KvService getKvService() {
        KeyValueStore store = new KeyValueStore() {
            @Override
            public void put(String key, VersionedValue value, String opId) {
                // no-op for these tests
            }

            @Override
            public VersionedValue get(String key) {
                return null;
            }

            @Override
            public List<VersionedValue> getSiblings(String key) {
                return Collections.emptyList();
            }
        };
        return  new KvService(store, "test-node");
    }

    private static CoordinatorService getCoordinatorService() {
        ClusterConfig.Node node = new ClusterConfig.Node("node-a", "localhost", PORT);
        var list = List.of(node);
        ClusterConfig cluster = new ClusterConfig(
                "node-a",
                list,
                1,
                1,
                1,
                128
        );
        RingRouting routing = new RingRouting(cluster);

        // ---- 3) NodeClient stub for coordinator behavior ----
        NodeClient failingClient = new NodeClient() {
            @Override
            public PutResult put(String nodeId, String key, String valueBase64, String coordNodeId) {
                // We want to exercise IllegalArgumentException -> 400 mapping for one test.
                throw new IllegalArgumentException("bad payload");
            }

            @Override
            public PutResult delete(String nodeId, String key, String coordNodeId) {
                throw new IllegalArgumentException("bad payload");
            }

            @Override
            public ReadResult get(String nodeId, String key) {
                // "not found" result; used only indirectly in GET tests
                return new ReadResult(false, null, Map.of());
            }
        };

        Map<String, NodeClient> clients = Map.of("node-a", failingClient);
        CoordinatorService coord = new CoordinatorService(cluster, routing, clients);
        return coord;
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop();
        }
    }

    private String baseUrl() {
        return "http://localhost:" + PORT;
    }

    @Test
    void empty_key_returns_400_for_get() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl() + "/kv/")) // no key suffix
                .GET()
                .build();

        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, resp.statusCode());
        assertTrue(resp.body().contains("key must not be empty"));
    }

    @Test
    void invalid_json_returns_400_for_put() throws Exception {
        String body = "{ invalid-json"; // intentionally broken

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl() + "/kv/user:1"))
                .PUT(HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json")
                .build();

        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, resp.statusCode());
        assertTrue(resp.body().contains("invalid JSON"));
    }

    @Test
    void too_large_body_returns_413_for_put() throws Exception {
        // Build a body larger than MAX_BODY_BYTES (10 MiB).
        int size = 11 * 1024 * 1024;
        String big = "x".repeat(size);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl() + "/kv/user:big"))
                .PUT(HttpRequest.BodyPublishers.ofString(big))
                .header("Content-Type", "application/json")
                .build();

        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(413, resp.statusCode());
        assertTrue(resp.body().contains("request body too large"));
    }

    @Test
    void illegal_argument_from_coordinator_maps_to_400() throws Exception {
        // Valid JSON, but our NodeClient stub throws IllegalArgumentException on put().
        String body = """
                {
                  "valueBase64": "dGVzdA==",
                  "nodeId": "coord-1"
                }
                """;

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl() + "/kv/user:42"))
                .PUT(HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json")
                .build();

        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, resp.statusCode());
        assertTrue(resp.body().contains("bad payload"));
    }
}
