package com.logagg.query.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Runtime statistics for a single RCA analysis.
 * Useful for debugging, billing estimation, and performance monitoring.
 */
@Data
@NoArgsConstructor
public class RcaStats {

    /** Number of S3 chunks downloaded during the retrieval phase. */
    @JsonProperty("chunks_read")
    private int chunksRead;

    /** Total log events deserialized from the downloaded chunks. */
    @JsonProperty("events_scanned")
    private int eventsScanned;

    /** Number of events that were actually included in the LLM context window. */
    @JsonProperty("events_used_for_context")
    private int eventsUsedForContext;

    /** Time taken for the LLM to generate the RCA report, in milliseconds. */
    @JsonProperty("llm_latency_ms")
    private long llmLatencyMs;

    /** Total wall-clock time for the entire RCA pipeline (retrieve + augment + generate). */
    @JsonProperty("total_latency_ms")
    private long totalLatencyMs;
}
