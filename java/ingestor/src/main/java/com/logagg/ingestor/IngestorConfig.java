package com.logagg.ingestor;

import com.logagg.common.model.LogEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.LinkedBlockingQueue;

@Configuration
public class IngestorConfig {

    @Value("${kafka.batch-size:100}")
    private int batchSize;

    /**
     * Shared event buffer. Capacity = batchSize * 10 = 1000 (matches Go implementation).
     */
    @Bean
    public LinkedBlockingQueue<LogEvent> eventBuffer() {
        return new LinkedBlockingQueue<>(batchSize * 10);
    }
}
