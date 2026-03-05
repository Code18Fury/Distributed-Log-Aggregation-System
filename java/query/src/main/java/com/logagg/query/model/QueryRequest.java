package com.logagg.query.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request body for POST /v1/query.
 * Field names match the Go QueryRequest JSON contract exactly.
 */
@Data
@NoArgsConstructor
public class QueryRequest {

    @JsonProperty("tenant_id")
    private String tenantId;

    @JsonProperty("service")
    private String service;

    @JsonProperty("from_ts")
    private long fromTs;

    @JsonProperty("to_ts")
    private long toTs;

    @JsonProperty("level")
    private String level;

    @JsonProperty("contains")
    private String contains;

    @JsonProperty("limit")
    private int limit;
}
