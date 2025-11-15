// file: src/main/java/io/dynlite/server/dto/GetResponse.java
package io.dynlite.server.dto;

import java.util.Map;

/**
 * JSON response for GET /kv/{key}.
 * When the key is found:
 *   {
 *     "found": true,
 *     "valueBase64": "aGVsbG8=",
 *     "vectorClock": { "node-a": 3, "node-b": 1 }
 *   }
 * When not found:
 *   {
 *     "found": false
 *   }
 */
public class GetResponse {
    public boolean found;
    public String valueBase64;      // present when found && !tombstone
    public Map<String, Integer> vectorClock;
}
