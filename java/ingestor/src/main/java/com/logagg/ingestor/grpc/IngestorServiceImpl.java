package com.logagg.ingestor.grpc;

import com.logagg.common.model.LogEvent;
import ingestor.v1.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

@GrpcService
public class IngestorServiceImpl extends IngestorServiceGrpc.IngestorServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(IngestorServiceImpl.class);

    private final LinkedBlockingQueue<LogEvent> eventBuffer;
    private final int batchSize;
    private final Counter eventsReceived;
    private final Counter eventsRejectedValidation;
    private final Counter eventsRejectedBuffer;
    private final Counter requestsTotal;

    public IngestorServiceImpl(
            LinkedBlockingQueue<LogEvent> eventBuffer,
            @Value("${kafka.batch-size:100}") int batchSize,
            MeterRegistry meterRegistry) {
        this.eventBuffer = eventBuffer;
        this.batchSize = batchSize;

        this.eventsReceived = Counter.builder("ingestor.events.received")
                .description("Total log events received")
                .register(meterRegistry);
        this.eventsRejectedValidation = Counter.builder("ingestor.events.rejected")
                .tag("reason", "validation_error")
                .register(meterRegistry);
        this.eventsRejectedBuffer = Counter.builder("ingestor.events.rejected")
                .tag("reason", "buffer_full")
                .register(meterRegistry);
        this.requestsTotal = Counter.builder("ingestor.requests.total")
                .description("Total ingestor requests")
                .register(meterRegistry);

        // Buffer depth gauge
        Gauge.builder("ingestor.buffer.depth", eventBuffer, LinkedBlockingQueue::size)
                .description("Current event buffer depth")
                .register(meterRegistry);
    }

    /**
     * Client-streaming RPC: client sends a stream of LogEvents, server sends one StreamAck.
     */
    @Override
    public StreamObserver<ingestor.v1.LogEvent> streamLogs(StreamObserver<StreamAck> responseObserver) {
        return new StreamObserver<>() {
            int acceptedCount = 0;
            int rejectedCount = 0;
            String lastError = "";

            @Override
            public void onNext(ingestor.v1.LogEvent protoEvent) {
                LogEvent event = protoToModel(protoEvent);
                try {
                    event.validate();
                    event.normalize();
                    if (eventBuffer.offer(event)) {
                        acceptedCount++;
                        eventsReceived.increment();
                    } else {
                        rejectedCount++;
                        lastError = "buffer full";
                        eventsRejectedBuffer.increment();
                        log.warn("Event rejected: buffer full for tenant={}", event.getTenantId());
                    }
                } catch (IllegalArgumentException e) {
                    rejectedCount++;
                    lastError = e.getMessage();
                    eventsRejectedValidation.increment();
                    log.warn("Invalid log event rejected: {}", e.getMessage());
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("Stream error", t);
                responseObserver.onError(Status.INTERNAL
                        .withDescription(t.getMessage())
                        .asRuntimeException());
            }

            @Override
            public void onCompleted() {
                responseObserver.onNext(StreamAck.newBuilder()
                        .setAcceptedCount(acceptedCount)
                        .setRejectedCount(rejectedCount)
                        .setLastError(lastError)
                        .build());
                responseObserver.onCompleted();
            }
        };
    }

    /**
     * Unary RPC: batch ingest for high-throughput clients.
     */
    @Override
    public void ingestBatch(IngestRequest request, StreamObserver<IngestResponse> responseObserver) {
        requestsTotal.increment();
        int accepted = 0, rejected = 0;
        String lastError = "";

        for (ingestor.v1.LogEvent protoEvent : request.getEventsList()) {
            LogEvent event = protoToModel(protoEvent);
            try {
                event.validate();
                event.normalize();
                if (eventBuffer.offer(event)) {
                    accepted++;
                    eventsReceived.increment();
                } else {
                    rejected++;
                    lastError = "buffer full";
                    eventsRejectedBuffer.increment();
                }
            } catch (IllegalArgumentException e) {
                rejected++;
                lastError = e.getMessage();
                eventsRejectedValidation.increment();
            }
        }

        responseObserver.onNext(IngestResponse.newBuilder()
                .setAcceptedCount(accepted)
                .setRejectedCount(rejected)
                .setLastError(lastError)
                .build());
        responseObserver.onCompleted();
    }

    /**
     * Unary RPC: health check — reports unhealthy if buffer is > 90% full.
     */
    @Override
    public void healthCheck(HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
        int bufferCapacity = batchSize * 10;
        double usage = (double) eventBuffer.size() / bufferCapacity;
        String status = usage > 0.9 ? "unhealthy" : "healthy";
        String message = usage > 0.9
                ? String.format("buffer is %.0f%% full", usage * 100)
                : "OK";

        responseObserver.onNext(HealthCheckResponse.newBuilder()
                .setStatus(status)
                .setMessage(message)
                .build());
        responseObserver.onCompleted();
    }

    private LogEvent protoToModel(ingestor.v1.LogEvent proto) {
        LogEvent event = new LogEvent();
        event.setTenantId(proto.getTenantId());
        event.setService(proto.getService());
        event.setHost(proto.getHost());
        event.setTimestamp(proto.getTimestamp());
        event.setLevel(proto.getLevel());
        event.setMessage(proto.getMessage());
        if (!proto.getLabelsMap().isEmpty()) {
            event.setLabels(new HashMap<>(proto.getLabelsMap()));
        }
        if (!proto.getTraceId().isEmpty()) {
            event.setTraceId(proto.getTraceId());
        }
        event.setIngestionTime(proto.getIngestionTime());
        return event;
    }
}
