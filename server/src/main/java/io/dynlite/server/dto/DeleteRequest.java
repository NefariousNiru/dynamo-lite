// file: src/main/java/io/dynlite/server/dto/DeleteRequest.java
package io.dynlite.server.dto;

/**
 * JSON body for DELETE /kv/{key}.
 * Example:
 *   {
 *     "nodeId": "node-a"
 *   }
 */
public class DeleteRequest {
    public String nodeId;
    public String opId;   // client-generated idempotency key
}
