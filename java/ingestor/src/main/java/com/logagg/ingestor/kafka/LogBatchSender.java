package com.logagg.ingestor.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.logagg.common.model.LogEvent;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Drains the event buffer and sends batches to Kafka.
 * Flushes every 1 second OR when the batch reaches batchSize — whichever comes first.
 * Runs on a fixed-delay schedule matching the Go processBatches goroutine.
 */
@Component
public class LogBatchSender {

    private static final Logger log = LoggerFactory.getLogger(LogBatchSender.class);

    private final LinkedBlockingQueue<LogEvent> eventBuffer;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final int batchSize;
    private final String kafkaTopic;
    private final Timer produceLatency;

    public LogBatchSender(
            LinkedBlockingQueue<LogEvent> eventBuffer,
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${kafka.batch-size:100}") int batchSize,
            @Value("${kafka.topic:logs.raw}") String kafkaTopic,
            MeterRegistry meterRegistry) {
        this.eventBuffer = eventBuffer;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = new ObjectMapper();
        this.batchSize = batchSize;
        this.kafkaTopic = kafkaTopic;
        this.produceLatency = Timer.builder("ingestor.kafka.produce.latency")
                .description("Kafka produce latency")
                .register(meterRegistry);
    }

    /**
     * Scheduled to run every 1000ms. Drains up to batchSize events from the buffer
     * and sends them to Kafka. Produces a message for each event with key = tenant:service.
     */
    @Scheduled(fixedDelayString = "${kafka.batch-timeout-ms:1000}")
    public void flushBatch() {
        List<LogEvent> batch = new ArrayList<>(batchSize);
        eventBuffer.drainTo(batch, batchSize);

        if (batch.isEmpty()) {
            return;
        }

        Timer.Sample sample = Timer.start();
        int sent = 0;

        for (LogEvent event : batch) {
            try {
                String key = event.getTenantId() + ":" + event.getService();
                String value = objectMapper.writeValueAsString(event);
                kafkaTemplate.send(kafkaTopic, key, value);
                sent++;
            } catch (Exception e) {
                log.error("Failed to serialize/send event to Kafka", e);
            }
        }

        sample.stop(produceLatency);
        log.debug("Sent batch of {} events to Kafka topic={}", sent, kafkaTopic);

        // If the buffer still has >= batchSize events, flush immediately again
        if (eventBuffer.size() >= batchSize) {
            flushBatch();
        }
    }
}
