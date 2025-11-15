// file: src/main/java/io/dynlite/server/dto/PutResponse.java
package io.dynlite.server.dto;


import java.util.Map;

/**
 * JSON response for PUT and DELETE operations.
 * Example for PUT:
 *   {
 *     "ok": true,
 *     "tombstone": false,
 *     "lwwMillis": 1728000000000,
 *     "vectorClock": { "node-a": 2 }
 *   }
 * Example for DELETE:
 *   {
 *     "ok": true,
 *     "tombstone": true,
 *     "lwwMillis": 1728000000500,
 *     "vectorClock": { "node-a": 3 }
 *   }
 */
public class PutResponse {
    public boolean ok;
    public boolean tombstone;
    public long lwwMillis;
    public Map<String,Integer> vectorClock;
}
