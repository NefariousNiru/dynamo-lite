package io.dynlite.server.dto;

/** JSON body for PUT /kv/{key}. */
public class PutRequest {
    public String valueBase64; // bytes encoded as Base64
    public String nodeId;      // which node is coordinating (usually this server)
}
