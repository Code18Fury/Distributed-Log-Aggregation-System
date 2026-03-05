package com.logagg.processor.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.logagg.common.model.LogEvent;
import com.logagg.common.util.ChunkUtils;
import com.logagg.processor.chunk.ChunkAggregator;
import com.logagg.processor.redis.RedisIndexService;
import com.logagg.processor.s3.S3Service;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Consumes log events from Kafka and accumulates them into time-windowed chunks.
 * A scheduled task flushes stale windows to S3 and updates the Redis index.
 *
 * This replicates the Go processor's ProcessMessage + flushBatch + processChunk logic.
 */
@Component
public class LogEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(LogEventConsumer.class);

    private final ChunkAggregator aggregator = new ChunkAggregator();
    private final ObjectMapper objectMapper;
    private final S3Service s3Service;
    private final RedisIndexService redisIndexService;
    private final int batchSize;
    private final Counter eventsConsumed;
    private final Counter chunksWritten;
    private final Counter s3WriteErrors;
    private final Counter redisIndexErrors;
    private final Timer processingLatency;

    public LogEventConsumer(
            S3Service s3Service,
            RedisIndexService redisIndexService,
            @Value("${kafka.batch-size:100}") int batchSize,
            MeterRegistry meterRegistry) {
        this.s3Service = s3Service;
        this.redisIndexService = redisIndexService;
        this.objectMapper = new ObjectMapper();
        this.batchSize = batchSize;

        this.eventsConsumed = Counter.builder("processor.events.consumed").register(meterRegistry);
        this.chunksWritten = Counter.builder("processor.chunks.written").register(meterRegistry);
        this.s3WriteErrors = Counter.builder("processor.s3.write.errors").register(meterRegistry);
        this.redisIndexErrors = Counter.builder("processor.redis.index.errors").register(meterRegistry);
        this.processingLatency = Timer.builder("processor.processing.latency").register(meterRegistry);
    }

    @KafkaListener(
            topics = "${kafka.topic:logs.raw}",
            groupId = "${kafka.consumer-group:processor-group}")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            LogEvent event = objectMapper.readValue(record.value(), LogEvent.class);
            aggregator.addEvent(event);
            eventsConsumed.increment();
        } catch (Exception e) {
            log.error("Failed to deserialize event at partition={} offset={}",
                    record.partition(), record.offset(), e);
        }
    }

    /**
     * Flush stale windows every 1 second (matches Go's batchTimer of 1s).
     * A window is flushed when it has >= batchSize events OR is >= 1s old.
     */
    @Scheduled(fixedDelayString = "${kafka.batch-timeout-ms:1000}")
    public void flushStaleWindows() {
        List<ChunkAggregator.ChunkWindow> windows =
                aggregator.drainFlushable(batchSize, 1000L);

        if (windows.isEmpty()) return;

        Timer.Sample sample = Timer.start();

        for (ChunkAggregator.ChunkWindow window : windows) {
            processWindow(window);
        }

        sample.stop(processingLatency);
        log.debug("Flushed {} windows", windows.size());
    }

    private void processWindow(ChunkAggregator.ChunkWindow window) {
        List<LogEvent> events = window.getEvents();
        if (events.isEmpty()) return;

        String s3Key = ChunkUtils.generateS3Key(
                window.getTenantId(), window.getService(), window.getWindowStart());

        try {
            byte[] ndjson = ChunkUtils.serializeToNdjson(events);
            s3Service.putObject(s3Key, ndjson);
            chunksWritten.increment();

            log.info("Uploaded chunk s3Key={} tenant={} service={} events={}",
                    s3Key, window.getTenantId(), window.getService(), events.size());
        } catch (Exception e) {
            s3WriteErrors.increment();
            log.error("Failed to write chunk to S3: s3Key={}", s3Key, e);
            return;
        }

        try {
            redisIndexService.indexChunk(
                    window.getTenantId(), window.getService(), window.getWindowStart(), s3Key);
        } catch (Exception e) {
            redisIndexErrors.increment();
            log.error("Failed to update Redis index for s3Key={}", s3Key, e);
        }
    }
}
