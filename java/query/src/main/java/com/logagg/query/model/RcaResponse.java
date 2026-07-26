package com.logagg.query.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Response body for POST /v1/rca.
 *
 * All narrative fields are LLM-generated and should be treated as a hypothesis,
 * not a definitive diagnosis. The 'confidence' field is self-reported by the model.
 */
@Data
@NoArgsConstructor
public class RcaResponse {

    /**
     * The most likely root cause of the incident, as identified from the log patterns.
     * Example: "Connection pool exhaustion in the payment service caused by a surge in
     * concurrent checkout requests."
     */
    @JsonProperty("root_cause")
    private String rootCause;

    /**
     * Secondary factors that amplified or contributed to the incident.
     * Example: ["No retry budget configured", "Missing circuit breaker on DB calls"]
     */
    @JsonProperty("contributing_factors")
    private List<String> contributingFactors;

    /**
     * Services identified in the logs as impacted during the incident window.
     * Example: ["payment-service", "order-service"]
     */
    @JsonProperty("affected_services")
    private List<String> affectedServices;

    /**
     * Actionable next steps to resolve or prevent recurrence.
     * Example: ["Increase max-pool-size from 10 to 50", "Add a circuit breaker with 60s timeout"]
     */
    @JsonProperty("remediation_steps")
    private List<String> remediationSteps;

    /**
     * LLM self-assessed confidence in its analysis: HIGH, MEDIUM, or LOW.
     * LOW typically means the log volume is too sparse to draw firm conclusions.
     */
    @JsonProperty("confidence")
    private String confidence;

    /** Operational stats for this RCA run (latency, events scanned, etc.). */
    @JsonProperty("stats")
    private RcaStats stats;
}
