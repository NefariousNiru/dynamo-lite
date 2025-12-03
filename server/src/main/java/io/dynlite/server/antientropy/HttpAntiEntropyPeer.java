// file: server/src/main/java/io/dynlite/server/antientropy/HttpAntiEntropyPeer.java
package io.dynlite.server.antientropy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dynlite.core.merkle.MerkleTree;
import io.dynlite.server.shard.ShardDescriptor;
import io.dynlite.server.shard.TokenRange;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

/**
 * HTTP-based AntiEntropyPeer.
 *
 * Talks to a remote node's admin endpoint:
 *
 *   GET /admin/anti-entropy/merkle-snapshot
 *       ?startToken=...&endToken=...&leafCount=...
 *
 * Response JSON:
 *
 *   {
 *     "rootHashBase64": "....",
 *     "leafCount": 1024,
 *     "digests": [
 *       { "token": 1234, "digestBase64": "..." },
 *       { "token": 5678, "digestBase64": "..." }
 *     ]
 *   }
 *
 * This class:
 *   - performs the HTTP GET,
 *   - parses JSON with Jackson,
 *   - converts into a MerkleSnapshotResponse with a List<KeyDigest>.
 *
 * Assumes MerkleTree.KeyDigest has a public constructor (long token, byte[] digest).
 */
public final class HttpAntiEntropyPeer implements AntiEntropyPeer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String remoteNodeId;
    private final URI baseUri;
    private final HttpClient client;

    public HttpAntiEntropyPeer(String remoteNodeId, URI baseUri) {
        this.remoteNodeId = Objects.requireNonNull(remoteNodeId, "remoteNodeId");
        this.baseUri = Objects.requireNonNull(baseUri, "baseUri");
        this.client = HttpClient.newHttpClient();
    }

    @Override
    public MerkleSnapshotResponse fetchMerkleSnapshot(ShardDescriptor shard, int leafCount) {
        TokenRange range = shard.range();

        String query = String.format(
                "startToken=%s&endToken=%s&leafCount=%d",
                encode(Long.toString(range.startInclusive())),
                encode(Long.toString(range.endExclusive())),
                leafCount
        );

        URI uri = baseUri.resolve("/admin/anti-entropy/merkle-snapshot?" + query);

        HttpRequest req = HttpRequest.newBuilder(uri).GET().build();

        try {
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new IOException(
                        "Remote " + remoteNodeId + " returned HTTP " + resp.statusCode()
                                + " for shard " + shard.shardId()
                );
            }

            String json = resp.body();
            SnapshotDto dto = MAPPER.readValue(json, SnapshotDto.TYPE_REF);

            byte[] rootHash = Base64.getDecoder().decode(dto.rootHashBase64());
            List<MerkleTree.KeyDigest> digests = new ArrayList<>(dto.digests().size());
            for (DigestDto d : dto.digests()) {
                long token = d.token();
                byte[] digest = Base64.getDecoder().decode(d.digestBase64());
                digests.add(new MerkleTree.KeyDigest(token, digest));
            }

            return new MerkleSnapshotResponse(
                    shard,
                    rootHash,
                    dto.leafCount(),
                    digests
            );
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(
                    "Failed to fetch Merkle snapshot from node "
                            + remoteNodeId + " for shard " + shard.shardId(),
                    e
            );
        }
    }

    private static String encode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    // ---------- JSON DTOs ----------

    public static final class SnapshotDto {
        static final TypeReference<SnapshotDto> TYPE_REF = new TypeReference<>() {};
        private final String rootHashBase64;
        private final int leafCount;
        private final List<DigestDto> digests;

        @JsonCreator
        public SnapshotDto(
                @JsonProperty("rootHashBase64") String rootHashBase64,
                @JsonProperty("leafCount") int leafCount,
                @JsonProperty("digests") List<DigestDto> digests
        ) {
            this.rootHashBase64 = rootHashBase64;
            this.leafCount = leafCount;
            this.digests = digests != null ? digests : List.of();
        }

        public String rootHashBase64() {
            return rootHashBase64;
        }

        public int leafCount() {
            return leafCount;
        }

        public List<DigestDto> digests() {
            return digests;
        }
    }

    public static final class DigestDto {
        private final long token;
        private final String digestBase64;

        @JsonCreator
        public DigestDto(
                @JsonProperty("token") long token,
                @JsonProperty("digestBase64") String digestBase64
        ) {
            this.token = token;
            this.digestBase64 = digestBase64;
        }

        public long token() {
            return token;
        }

        public String digestBase64() {
            return digestBase64;
        }
    }
}
