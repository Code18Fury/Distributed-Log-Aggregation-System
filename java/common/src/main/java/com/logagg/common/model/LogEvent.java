package com.logagg.common.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LogEvent {

    @JsonProperty("tenant_id")
    private String tenantId;

    @JsonProperty("service")
    private String service;

    @JsonProperty("host")
    private String host;

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("level")
    private String level;

    @JsonProperty("message")
    private String message;

    @JsonProperty("labels")
    private Map<String, String> labels;

    @JsonProperty("trace_id")
    private String traceId;

    @JsonProperty("ingestion_time")
    private long ingestionTime;

    /**
     * Validates required fields and timestamp bounds.
     * Throws IllegalArgumentException if validation fails.
     */
    public void validate() {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenant_id is required");
        }
        if (service == null || service.isBlank()) {
            throw new IllegalArgumentException("service is required");
        }
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host is required");
        }
        if (message == null || message.isBlank()) {
            throw new IllegalArgumentException("message is required");
        }
        if (timestamp <= 0) {
            throw new IllegalArgumentException("timestamp must be positive");
        }
        long maxFutureMs = Instant.now().plusSeconds(3600).toEpochMilli();
        if (timestamp > maxFutureMs) {
            throw new IllegalArgumentException("timestamp is too far in the future");
        }
    }

    /**
     * Normalizes fields: trims whitespace, uppercases level, defaults to INFO if invalid,
     * sets ingestion_time if not already set.
     */
    public void normalize() {
        if (tenantId != null) tenantId = tenantId.strip();
        if (service != null) service = service.strip();
        if (host != null) host = host.strip();
        if (message != null) message = message.strip();

        if (level != null) {
            level = level.toUpperCase();
        }
        try {
            LogLevel.valueOf(level);
        } catch (Exception e) {
            level = LogLevel.INFO.name();
        }

        if (ingestionTime == 0) {
            ingestionTime = Instant.now().toEpochMilli();
        }
    }
}
