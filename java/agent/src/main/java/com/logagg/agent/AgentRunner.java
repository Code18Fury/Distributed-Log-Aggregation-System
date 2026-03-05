package com.logagg.agent;

import ingestor.v1.IngestorServiceGrpc;
import ingestor.v1.LogEvent;
import ingestor.v1.StreamAck;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Synthetic log generator — connects to the ingestor via gRPC and streams
 * random log events. Replicates the Go agent behavior exactly.
 *
 * Configuration (env vars or application.yml):
 *   INGESTOR_ADDR    default: localhost:50051
 *   AGENT_TENANT     default: demo-tenant
 *   AGENT_SERVICE    default: demo-service
 *   AGENT_RATE       default: 10  (events/second)
 *   AGENT_DURATION   default: 60  (seconds, 0 = unlimited)
 */
@Component
public class AgentRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(AgentRunner.class);

    private static final List<String> MESSAGES = List.of(
            "User login successful",
            "Database query executed",
            "Cache hit for key user:123",
            "API request processed",
            "Background job completed",
            "Connection timeout to external service",
            "Payment transaction processed",
            "Email sent successfully",
            "File uploaded to storage",
            "Authentication failed"
    );

    private static final List<String> LEVELS = List.of("DEBUG", "INFO", "WARN", "ERROR");

    @Value("${agent.addr:localhost:50051}")
    private String ingestorAddr;

    @Value("${agent.tenant:demo-tenant}")
    private String tenantId;

    @Value("${agent.service:demo-service}")
    private String service;

    @Value("${agent.rate:10}")
    private int eventsPerSecond;

    @Value("${agent.duration:60}")
    private int durationSeconds;

    @Override
    public void run(String... args) throws Exception {
        log.info("Starting log generator addr={} tenant={} service={} rate={} duration={}",
                ingestorAddr, tenantId, service, eventsPerSecond, durationSeconds);

        // Parse host:port
        String[] parts = ingestorAddr.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 50051;

        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();

        try {
            IngestorServiceGrpc.IngestorServiceStub stub = IngestorServiceGrpc.newStub(channel);
            CountDownLatch finishLatch = new CountDownLatch(1);

            StreamObserver<StreamAck> responseObserver = new StreamObserver<>() {
                @Override
                public void onNext(StreamAck ack) {
                    log.info("Final ack: accepted={} rejected={} lastError={}",
                            ack.getAcceptedCount(), ack.getRejectedCount(), ack.getLastError());
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Stream error", t);
                    finishLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    finishLatch.countDown();
                }
            };

            StreamObserver<LogEvent> requestObserver = stub.streamLogs(responseObserver);

            Random random = new Random();
            long intervalMs = 1000L / eventsPerSecond;
            long startMs = System.currentTimeMillis();
            int totalSent = 0;

            try {
                while (true) {
                    LogEvent event = LogEvent.newBuilder()
                            .setTenantId(tenantId)
                            .setService(service)
                            .setHost("generator-1")
                            .setTimestamp(System.currentTimeMillis())
                            .setLevel(LEVELS.get(random.nextInt(LEVELS.size())))
                            .setMessage(MESSAGES.get(random.nextInt(MESSAGES.size())))
                            .putLabels("environment", "development")
                            .putLabels("version", "1.0.0")
                            .build();

                    requestObserver.onNext(event);
                    totalSent++;

                    if (totalSent % 100 == 0) {
                        long elapsed = System.currentTimeMillis() - startMs;
                        double actualRate = (double) totalSent / (elapsed / 1000.0);
                        log.info("Progress: total_sent={} actual_rate={}/s",
                                totalSent, String.format("%.2f", actualRate));
                    }

                    if (durationSeconds > 0) {
                        long elapsed = System.currentTimeMillis() - startMs;
                        if (elapsed >= (long) durationSeconds * 1000) {
                            log.info("Duration limit reached. total_sent={}", totalSent);
                            break;
                        }
                    }

                    Thread.sleep(intervalMs);
                }
            } finally {
                requestObserver.onCompleted();
            }

            finishLatch.await(30, TimeUnit.SECONDS);
        } finally {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
