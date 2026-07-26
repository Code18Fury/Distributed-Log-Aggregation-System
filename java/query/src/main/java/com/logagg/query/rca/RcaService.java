package com.logagg.query.rca;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.logagg.common.model.LogEvent;
import com.logagg.query.model.RcaRequest;
import com.logagg.query.model.RcaResponse;
import com.logagg.query.model.RcaStats;
import com.logagg.query.service.QueryService;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatLanguageModel;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * RAG-based Root Cause Analysis service.
 *
 * Implements the three-phase RAG pipeline:
 *   1. RETRIEVE — fetches relevant log events from S3 via the Redis ZSET index.
 *   2. AUGMENT  — builds a structured prompt embedding the retrieved logs as context.
 *   3. GENERATE — calls the Gemini LLM and parses the structured JSON response.
 *
 * This service deliberately keeps each phase separate for testability and clarity.
 */
@Service
public class RcaService {

    private static final Logger log = LoggerFactory.getLogger(RcaService.class);

    private final QueryService queryService;
    private final ChatLanguageModel chatModel;
    private final ObjectMapper objectMapper;
    private final int maxContextEvents;

    // Micrometer metrics
    private final Counter rcaRequestsTotal;
    private final Counter rcaSuccessTotal;
    private final Counter rcaFailureTotal;
    private final Timer   rcaTotalLatency;
    private final Timer   rcaLlmLatency;

    public RcaService(
            QueryService queryService,
            ChatLanguageModel chatModel,
            @Value("${rca.max-context-events:200}") int maxContextEvents,
            MeterRegistry meterRegistry) {
        this.queryService      = queryService;
        this.chatModel         = chatModel;
        this.objectMapper      = new ObjectMapper();
        this.maxContextEvents  = maxContextEvents;

        this.rcaRequestsTotal  = Counter.builder("rca.requests.total").register(meterRegistry);
        this.rcaSuccessTotal   = Counter.builder("rca.requests.success").register(meterRegistry);
        this.rcaFailureTotal   = Counter.builder("rca.requests.failure").register(meterRegistry);
        this.rcaTotalLatency   = Timer.builder("rca.latency.total").register(meterRegistry);
        this.rcaLlmLatency     = Timer.builder("rca.latency.llm").register(meterRegistry);
    }

    /**
     * Executes the full RAG pipeline for a given incident scope.
     *
     * @param request  The RCA request containing tenant, service, and time window.
     * @return         A structured {@link RcaResponse} with the LLM-generated analysis.
     */
    public RcaResponse analyze(RcaRequest request) {
        rcaRequestsTotal.increment();
        RcaStats stats = new RcaStats();
        Timer.Sample totalSample = Timer.start();

        try {
            // ---------------------------------------------------------------
            // PHASE 1: RETRIEVE
            // Reuse QueryService to fetch log events from Redis index + S3.
            // ---------------------------------------------------------------
            log.info("RCA retrieve phase: tenant={} service={} from={} to={}",
                    request.getTenantId(), request.getService(),
                    request.getFromTs(), request.getToTs());

            QueryService.RetrievalResult retrieved = queryService.retrieveLogsForRca(
                    request.getTenantId(),
                    request.getService(),
                    request.getFromTs(),
                    request.getToTs(),
                    maxContextEvents);

            stats.setChunksRead(retrieved.chunksRead());
            stats.setEventsScanned(retrieved.eventsScanned());
            stats.setEventsUsedForContext(retrieved.events().size());

            log.info("RCA retrieve complete: chunks={} scanned={} context={}",
                    retrieved.chunksRead(), retrieved.eventsScanned(), retrieved.events().size());

            if (retrieved.events().isEmpty()) {
                log.warn("RCA: no log events found for tenant={} service={}",
                        request.getTenantId(), request.getService());
                return buildEmptyResponse(stats);
            }

            // ---------------------------------------------------------------
            // PHASE 2: AUGMENT
            // Build the structured prompt from the retrieved log context.
            // ---------------------------------------------------------------
            String systemPrompt = RcaPromptBuilder.buildSystemPrompt();
            String userPrompt   = RcaPromptBuilder.buildUserPrompt(
                    retrieved.events(),
                    request.getTenantId(),
                    request.getService(),
                    request.getFromTs(),
                    request.getToTs(),
                    request.getIncidentDescription());

            log.debug("RCA prompt user-section length: {} chars", userPrompt.length());

            // ---------------------------------------------------------------
            // PHASE 3: GENERATE
            // Call the LLM and parse the structured JSON response.
            // ---------------------------------------------------------------
            Timer.Sample llmSample = Timer.start();
            String rawJson;
            try {
                rawJson = chatModel.generate(
                        SystemMessage.from(systemPrompt),
                        UserMessage.from(userPrompt)
                ).content().text();
            } finally {
                long llmNs = llmSample.stop(rcaLlmLatency);
                stats.setLlmLatencyMs(llmNs / 1_000_000);
            }

            log.debug("RCA raw LLM response: {}", rawJson);

            RcaResponse response = parseRcaJson(rawJson);
            response.setStats(stats);

            rcaSuccessTotal.increment();
            return response;

        } catch (Exception e) {
            rcaFailureTotal.increment();
            log.error("RCA pipeline failed for tenant={} service={}",
                    request.getTenantId(), request.getService(), e);
            throw new RuntimeException("RCA analysis failed: " + e.getMessage(), e);
        } finally {
            long totalNs = totalSample.stop(rcaTotalLatency);
            stats.setTotalLatencyMs(totalNs / 1_000_000);
        }
    }

    // -----------------------------------------------------------------------
    // Private Helpers
    // -----------------------------------------------------------------------

    /**
     * Parses the LLM's JSON output into a typed RcaResponse.
     * Strips markdown code fences if the model wrapped its response in them.
     */
    private RcaResponse parseRcaJson(String rawJson) throws Exception {
        // Some LLMs wrap JSON in ```json ... ``` blocks even when told not to.
        String cleaned = rawJson.strip();
        if (cleaned.startsWith("```")) {
            cleaned = cleaned.replaceAll("```[a-zA-Z]*\\n?", "").strip();
            if (cleaned.endsWith("```")) {
                cleaned = cleaned.substring(0, cleaned.lastIndexOf("```")).strip();
            }
        }

        Map<String, Object> parsed = objectMapper.readValue(
                cleaned, new TypeReference<>() {});

        RcaResponse response = new RcaResponse();
        response.setRootCause(getStr(parsed, "root_cause"));
        response.setConfidence(getStr(parsed, "confidence"));

        Object cf = parsed.get("contributing_factors");
        response.setContributingFactors(castStringList(cf));

        Object as = parsed.get("affected_services");
        response.setAffectedServices(castStringList(as));

        Object rs = parsed.get("remediation_steps");
        response.setRemediationSteps(castStringList(rs));

        return response;
    }

    /** Returns a fallback response when no logs are found for the requested window. */
    private RcaResponse buildEmptyResponse(RcaStats stats) {
        RcaResponse response = new RcaResponse();
        response.setRootCause("No log events found for the specified time window and service. "
                + "Verify the tenant_id, service name, and time range.");
        response.setContributingFactors(Collections.emptyList());
        response.setAffectedServices(Collections.emptyList());
        response.setRemediationSteps(List.of("Confirm that the agent is running and shipping logs to the ingestor.",
                "Check that the processor has flushed chunks to S3 for this time range."));
        response.setConfidence("LOW");
        response.setStats(stats);
        return response;
    }

    @SuppressWarnings("unchecked")
    private List<String> castStringList(Object obj) {
        if (obj instanceof List<?> list) {
            return (List<String>) list;
        }
        return Collections.emptyList();
    }

    private String getStr(Map<String, Object> map, String key) {
        Object v = map.get(key);
        return v == null ? "" : v.toString();
    }
}
