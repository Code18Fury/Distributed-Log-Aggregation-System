package com.logagg.query.rca;

import com.logagg.common.model.LogEvent;

import java.time.Instant;
import java.util.List;

/**
 * Handles all prompt engineering for the RCA Copilot.
 *
 * Keeps prompt construction decoupled from the service layer so that
 * prompt templates can be tuned independently without touching business logic.
 */
public final class RcaPromptBuilder {

    // Max characters per log message line included in the prompt.
    // Truncating prevents a single verbose log line from dominating the context.
    private static final int MAX_MESSAGE_LENGTH = 200;

    private RcaPromptBuilder() {}

    /**
     * Builds the system prompt that defines the LLM's persona and output schema.
     *
     * The system prompt instructs the model to act as a senior SRE, constrains
     * the response to a strict JSON schema, and prevents hallucination by telling
     * the model to only reference evidence found in the provided logs.
     */
    public static String buildSystemPrompt() {
        return """
                You are a senior Site Reliability Engineer (SRE) and distributed systems expert.
                You are performing Root Cause Analysis (RCA) on a set of application logs from a distributed microservices system.

                Your responsibilities:
                - Identify the most likely root cause of the incident from the log evidence.
                - List secondary contributing factors (configuration, missing safeguards, cascading failures).
                - Identify which services appear affected based on log data.
                - Provide specific, actionable remediation steps.
                - Self-assess your confidence as HIGH, MEDIUM, or LOW based on log volume and signal quality.

                Rules:
                - Base your analysis ONLY on the log evidence provided. Do not invent facts.
                - If logs are too sparse to draw a firm conclusion, set confidence to LOW and say so.
                - Be concise and technical. Use engineering terminology.

                You MUST respond with ONLY valid JSON matching this exact schema:
                {
                  "root_cause": "<string>",
                  "contributing_factors": ["<string>", ...],
                  "affected_services": ["<string>", ...],
                  "remediation_steps": ["<string>", ...],
                  "confidence": "HIGH" | "MEDIUM" | "LOW"
                }
                Do not include any text, markdown, or explanation outside the JSON object.
                """;
    }

    /**
     * Builds the user-facing prompt that contains the actual log context.
     *
     * @param events              The retrieved log events to include as context.
     * @param tenantId            The tenant ID, for grounding the analysis.
     * @param service             The service name, for grounding the analysis.
     * @param fromTs              Incident window start (epoch ms).
     * @param toTs                Incident window end (epoch ms).
     * @param incidentDescription Optional plain-text symptom description from the user.
     */
    public static String buildUserPrompt(
            List<LogEvent> events,
            String tenantId,
            String service,
            long fromTs,
            long toTs,
            String incidentDescription) {

        StringBuilder sb = new StringBuilder();

        // --- Incident Context ---
        sb.append("## Incident Context\n");
        sb.append("- Tenant: ").append(tenantId).append("\n");
        sb.append("- Primary Service: ").append(service).append("\n");
        sb.append("- Window: ")
          .append(Instant.ofEpochMilli(fromTs))
          .append(" to ")
          .append(Instant.ofEpochMilli(toTs))
          .append("\n");
        sb.append("- Total Log Events Retrieved: ").append(events.size()).append("\n");

        if (incidentDescription != null && !incidentDescription.isBlank()) {
            sb.append("- Reported Symptoms: ").append(incidentDescription.strip()).append("\n");
        }

        // --- Log Summary Statistics ---
        long errorCount = events.stream()
                .filter(e -> "ERROR".equalsIgnoreCase(e.getLevel()))
                .count();
        long warnCount = events.stream()
                .filter(e -> "WARN".equalsIgnoreCase(e.getLevel()))
                .count();
        sb.append("\n## Log Summary\n");
        sb.append("- ERROR events: ").append(errorCount).append("\n");
        sb.append("- WARN events:  ").append(warnCount).append("\n");
        sb.append("- Other events: ").append(events.size() - errorCount - warnCount).append("\n");

        // --- Log Dump (ERROR/WARN first for signal clarity) ---
        sb.append("\n## Log Events (ERROR/WARN prioritized)\n");
        sb.append("Format: [timestamp] LEVEL service | message\n\n");

        // Prioritize high-signal events so the LLM sees them first within token limits.
        events.stream()
                .filter(e -> "ERROR".equalsIgnoreCase(e.getLevel()) || "WARN".equalsIgnoreCase(e.getLevel()))
                .forEach(e -> sb.append(formatLogLine(e)).append("\n"));

        // Then include remaining INFO/DEBUG events for temporal context.
        events.stream()
                .filter(e -> !"ERROR".equalsIgnoreCase(e.getLevel()) && !"WARN".equalsIgnoreCase(e.getLevel()))
                .forEach(e -> sb.append(formatLogLine(e)).append("\n"));

        sb.append("\n## Task\n");
        sb.append("Analyze the logs above and produce the RCA JSON as instructed.");

        return sb.toString();
    }

    /**
     * Formats a single log event as a compact, readable line.
     * Truncates long messages to keep the context window manageable.
     */
    private static String formatLogLine(LogEvent event) {
        String timestamp = Instant.ofEpochMilli(event.getTimestamp()).toString();
        String level     = event.getLevel() == null ? "INFO" : event.getLevel();
        String svc       = event.getService() == null ? "unknown" : event.getService();
        String msg       = event.getMessage() == null ? "" : event.getMessage();

        if (msg.length() > MAX_MESSAGE_LENGTH) {
            msg = msg.substring(0, MAX_MESSAGE_LENGTH) + "…";
        }

        return String.format("[%s] %-5s %s | %s", timestamp, level, svc, msg);
    }
}
