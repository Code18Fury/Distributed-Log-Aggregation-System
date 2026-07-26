package com.logagg.query.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request body for POST /v1/rca.
 * Identifies the incident scope and optional plain-text hint for the LLM.
 */
@Data
@NoArgsConstructor
public class RcaRequest {

    /** The tenant whose logs will be retrieved. Required. */
    @JsonProperty("tenant_id")
    private String tenantId;

    /** The service name to scope the log retrieval. Required. */
    @JsonProperty("service")
    private String service;

    /** Incident window start — epoch milliseconds. Required. */
    @JsonProperty("from_ts")
    private long fromTs;

    /** Incident window end — epoch milliseconds. Required. */
    @JsonProperty("to_ts")
    private long toTs;

    /**
     * Optional plain-English description of the observed incident symptoms.
     * When provided, this is injected into the LLM prompt to focus the analysis
     * (e.g. "Users are reporting login failures and 503 responses").
     */
    @JsonProperty("incident_description")
    private String incidentDescription;
}
