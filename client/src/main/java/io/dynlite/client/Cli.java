// file: client/src/main/java/io/dynlite/client/Cli.java
package io.dynlite.client;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/**
 * Simple CLI for interacting with a running DynamoLite node over HTTP.
 *
 * Usage:
 *   dynlite-cli [--base-url http://host:port] put <key> <value>
 *   dynlite-cli [--base-url http://host:port] get <key>
 *   dynlite-cli [--base-url http://host:port] del <key>
 *
 * Examples:
 *   dynlite-cli put foo bar
 *   dynlite-cli get foo
 *   dynlite-cli del foo
 */
public final class Cli {

    private static final String DEFAULT_BASE_URL = "http://localhost:8080";

    private final HttpClient http;
    private final String baseUrl;

    private Cli(String baseUrl) {
        this.http = HttpClient.newHttpClient();
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }

    public static void main(String[] args) {
        try {
            if (args.length == 0) {
                usageAndExit("missing command");
            }

            Map.Entry<String, String[]> parsed = parseBaseUrl(args);
            String baseUrl = parsed.getKey();
            String[] rest = parsed.getValue();

            if (rest.length == 0) {
                usageAndExit("missing command");
            }

            String cmd = rest[0];
            Cli cli = new Cli(baseUrl);

            switch (cmd) {
                case "put" -> {
                    if (rest.length != 3) {
                        usageAndExit("put requires <key> <value>");
                    }
                    String key = rest[1];
                    String value = rest[2];
                    cli.put(key, value);
                }
                case "get" -> {
                    if (rest.length != 2) {
                        usageAndExit("get requires <key>");
                    }
                    String key = rest[1];
                    cli.get(key);
                }
                case "del" -> {
                    if (rest.length != 2) {
                        usageAndExit("del requires <key>");
                    }
                    String key = rest[1];
                    cli.del(key);
                }
                default -> usageAndExit("unknown command: " + cmd);
            }
        } catch (CliException e) {
            System.err.println("error: " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            System.exit(2);
        }
    }

    private static Map.Entry<String, String[]> parseBaseUrl(String[] args) {
        if (args.length >= 2 && "--base-url".equals(args[0])) {
            if (args.length < 3) {
                usageAndExit("--base-url requires a value");
            }
            String baseUrl = args[1];
            String[] rest = new String[args.length - 2];
            System.arraycopy(args, 2, rest, 0, rest.length);
            return Map.entry(baseUrl, rest);
        }
        return Map.entry(DEFAULT_BASE_URL, args);
    }

    private void put(String key, String value) throws Exception {
        String valueB64 = Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
        String body = "{\"valueBase64\":\"" + valueB64 + "\",\"nodeId\":null,\"opId\":null}";

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/kv/" + key))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new CliException("PUT failed (" + resp.statusCode() + "): " + resp.body());
        }
        System.out.println("OK");
    }

    private void get(String key) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/kv/" + key))
                .GET()
                .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() == 404) {
            System.out.println("(not found)");
            return;
        }
        if (resp.statusCode() != 200) {
            throw new CliException("GET failed (" + resp.statusCode() + "): " + resp.body());
        }

        // Very small, ad-hoc "parsing" to avoid JSON deps: assume response is
        // { "found":true, "valueBase64":"..." ... }
        String body = resp.body();
        String marker = "\"valueBase64\"";
        int idx = body.indexOf(marker);
        if (idx < 0) {
            System.out.println(body);
            return;
        }
        int colon = body.indexOf(':', idx);
        int firstQuote = body.indexOf('"', colon + 1);
        int secondQuote = body.indexOf('"', firstQuote + 1);
        if (firstQuote < 0 || secondQuote < 0) {
            System.out.println(body);
            return;
        }
        String valueB64 = body.substring(firstQuote + 1, secondQuote);
        byte[] decoded = Base64.getDecoder().decode(valueB64);
        String value = new String(decoded, StandardCharsets.UTF_8);
        System.out.println(value);
    }

    private void del(String key) throws Exception {
        String body = "{\"nodeId\":null,\"opId\":null}";

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/kv/" + key))
                .header("Content-Type", "application/json")
                .method("DELETE", HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new CliException("DEL failed (" + resp.statusCode() + "): " + resp.body());
        }
        System.out.println("OK");
    }

    private static void usageAndExit(String msg) {
        if (msg != null && !msg.isBlank()) {
            System.err.println("error: " + msg);
        }
        System.err.println("""
                Usage:
                  dynlite-cli [--base-url http://host:port] put <key> <value>
                  dynlite-cli [--base-url http://host:port] get <key>
                  dynlite-cli [--base-url http://host:port] del <key>
                """);
        System.exit(1);
    }

    private static final class CliException extends RuntimeException {
        CliException(String msg) {
            super(msg);
        }
    }
}
