// file: src/main/java/io/dynlite/server/dto/PutRequest.java
package io.dynlite.server.dto;

/**
 * JSON body for PUT /kv/{key}.
 * Example:
 *   {
 *     "valueBase64": "aGVsbG8=",
 *     "nodeId": "node-a"
 *   }
 */
public class PutRequest {
    public String valueBase64; // bytes encoded as Base64
    public String nodeId;      // which node is coordinating (usually this server)
    public String opId;        // client-generated idempotency key
}
