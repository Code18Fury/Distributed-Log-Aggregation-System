package com.logagg.query.service;

import com.logagg.common.model.LogEvent;
import com.logagg.common.util.ChunkUtils;
import com.logagg.query.model.QueryRequest;
import com.logagg.query.model.QueryResponse;
import com.logagg.query.model.QueryStats;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


/**
 * Executes log queries by:
 *   1. Fetching S3 keys from the Redis ZSET index (time-range lookup)
 *   2. Downloading + decompressing chunks from S3
 *   3. Deserializing NDJSON and applying filters
 *
 * Exactly mirrors the Go executeQuery logic.
 */

@Service
public class QueryService {

    private static final Logger log = LoggerFactory.getLogger(QueryService.class);
    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 1000;

    private final StringRedisTemplate redisTemplate;
    private final S3Client s3Client;
    private final String bucket;
    private final Counter s3ObjectsRead;
    private final Counter eventsScanned;
    private final Counter requestsTotal;
    private final Timer queryLatency;

    public QueryService(
            StringRedisTemplate redisTemplate,
            S3Client s3Client,
            @Value("${s3.bucket:logs-bucket}") String bucket,
            MeterRegistry meterRegistry) {
        this.redisTemplate = redisTemplate;
        this.s3Client = s3Client;
        this.bucket = bucket;

        this.s3ObjectsRead = Counter.builder("query.s3.objects.read").register(meterRegistry);
        this.eventsScanned = Counter.builder("query.events.scanned").register(meterRegistry);
        this.requestsTotal = Counter.builder("query.requests.total").register(meterRegistry);
        this.queryLatency = Timer.builder("query.latency").register(meterRegistry);
    }

    public QueryResponse query(QueryRequest req) {
        if (req.getLimit() <= 0) req.setLimit(DEFAULT_LIMIT);
        if (req.getLimit() > MAX_LIMIT) req.setLimit(MAX_LIMIT);

        QueryStats stats = new QueryStats();
        List<LogEvent> results = new ArrayList<>();

        Timer.Sample sample = Timer.start();

        String zsetKey = "idx:" + req.getTenantId() + ":" + req.getService();
        Set<String> s3Keys = redisTemplate.opsForZSet()
                .rangeByScore(zsetKey, req.getFromTs(), req.getToTs());

        if (s3Keys == null || s3Keys.isEmpty()) {
            log.debug("No chunks found for tenant={} service={}", req.getTenantId(), req.getService());
        } else {
            for (String s3Key : s3Keys) {
                if (results.size() >= req.getLimit()) break;

                byte[] decompressed = downloadAndDecompress(s3Key);
                if (decompressed == null) continue;

                stats.setObjectsRead(stats.getObjectsRead() + 1);
                s3ObjectsRead.increment();

                try {
                    List<LogEvent> events = ChunkUtils.deserializeFromNdjson(decompressed);
                    for (LogEvent event : events) {
                        stats.setEventsScanned(stats.getEventsScanned() + 1);
                        eventsScanned.increment();

                        if (matches(event, req)) {
                            results.add(event);
                            if (results.size() >= req.getLimit()) break;
                        }
                    }
                } catch (Exception e) {
                    log.error("Failed to deserialize chunk s3Key={}", s3Key, e);
                }
            }
        }

        long durationNs = sample.stop(queryLatency); // returns nanoseconds
        long durationMs = durationNs / 1_000_000;
        stats.setEventsReturned(results.size());
        stats.setDurationMs(durationMs);
        requestsTotal.increment();

        QueryResponse response = new QueryResponse();
        response.setItems(results);
        response.setStats(stats);
        return response;
    }

    /**
     * Retrieval phase for the RCA pipeline.
     *
     * Fetches up to {@code maxEvents} log events from S3 chunks in the given time window,
     * prioritizing ERROR and WARN events so they appear first in the LLM context.
     *
     * @param tenantId   Tenant scope.
     * @param service    Service scope.
     * @param fromTs     Window start, epoch milliseconds.
     * @param toTs       Window end, epoch milliseconds.
     * @param maxEvents  Maximum number of events to return (token-budget guard).
     * @return           A {@link RetrievalResult} containing events and telemetry.
     */
    public RetrievalResult retrieveLogsForRca(
            String tenantId, String service, long fromTs, long toTs, int maxEvents) {

        List<LogEvent> allEvents = new ArrayList<>();
        int chunksRead    = 0;
        int eventsScanned = 0;

        String zsetKey = "idx:" + tenantId + ":" + service;
        Set<String> s3Keys = redisTemplate.opsForZSet().rangeByScore(zsetKey, fromTs, toTs);

        if (s3Keys != null) {
            for (String s3Key : s3Keys) {
                byte[] decompressed = downloadAndDecompress(s3Key);
                if (decompressed == null) continue;
                chunksRead++;

                try {
                    List<LogEvent> chunk = ChunkUtils.deserializeFromNdjson(decompressed);
                    eventsScanned += chunk.size();
                    allEvents.addAll(chunk);
                } catch (Exception e) {
                    log.error("RCA: failed to deserialize chunk s3Key={}", s3Key, e);
                }
            }
        }

        // Sort: ERROR first, then WARN, then others — maximises signal density
        // within the LLM token budget.
        List<LogEvent> sorted = allEvents.stream()
                .sorted((a, b) -> levelPriority(a.getLevel()) - levelPriority(b.getLevel()))
                .limit(maxEvents)
                .toList();

        return new RetrievalResult(sorted, chunksRead, eventsScanned);
    }

    /** Lower value = higher priority in the context window. */
    private int levelPriority(String level) {
        if (level == null) return 3;
        return switch (level.toUpperCase()) {
            case "ERROR" -> 0;
            case "WARN"  -> 1;
            case "INFO"  -> 2;
            default      -> 3;
        };
    }

    byte[] downloadAndDecompress(String s3Key) {
        try {
            ResponseBytes<GetObjectResponse> responseBytes = s3Client.getObjectAsBytes(
                    GetObjectRequest.builder().bucket(bucket).key(s3Key).build());
            return ChunkUtils.decompress(responseBytes.asByteArray());
        } catch (Exception e) {
            log.error("Failed to download/decompress s3Key={}", s3Key, e);
            return null;
        }
    }

    /**
     * Result record for the RCA retrieval phase.
     *
     * @param events        Prioritised, capped list of log events for LLM context.
     * @param chunksRead    Number of S3 chunks downloaded.
     * @param eventsScanned Total events deserialized before the cap was applied.
     */
    public record RetrievalResult(List<LogEvent> events, int chunksRead, int eventsScanned) {}

    private boolean matches(LogEvent event, QueryRequest req) {
        if (event.getTimestamp() < req.getFromTs() || event.getTimestamp() > req.getToTs()) {
            return false;
        }
        if (!req.getTenantId().equals(event.getTenantId())) {
            return false;
        }
        if (req.getService() != null && !req.getService().isBlank()
                && !req.getService().equals(event.getService())) {
            return false;
        }
        if (req.getLevel() != null && !req.getLevel().isBlank()
                && !req.getLevel().equalsIgnoreCase(event.getLevel())) {
            return false;
        }
        if (req.getContains() != null && !req.getContains().isBlank()) {
            String msg = event.getMessage() == null ? "" : event.getMessage().toLowerCase();
            if (!msg.contains(req.getContains().toLowerCase())) {
                return false;
            }
        }
        return true;
    }
}
