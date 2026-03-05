package com.logagg.processor.chunk;

import com.logagg.common.model.LogEvent;
import com.logagg.common.util.ChunkUtils;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Groups incoming log events by (tenantId, service, 5-minute time window).
 * The key format is "tenantId:service:windowStart", matching the Go chunker.
 */
public class ChunkAggregator {

    private final ConcurrentHashMap<String, ChunkWindow> windows = new ConcurrentHashMap<>();

    /**
     * Adds an event to the appropriate time window.
     */
    public void addEvent(LogEvent event) {
        long windowStart = ChunkUtils.getWindowStart(event.getTimestamp());
        String key = event.getTenantId() + ":" + event.getService() + ":" + windowStart;
        windows.computeIfAbsent(key, k -> new ChunkWindow(event.getTenantId(), event.getService(), windowStart))
                .addEvent(event);
    }

    /**
     * Returns all windows that have at least minEvents events or are older than maxAgeMs.
     * Removes them from the aggregator so they can be flushed.
     */
    public List<ChunkWindow> drainFlushable(int minEvents, long maxAgeMs) {
        List<ChunkWindow> result = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (Map.Entry<String, ChunkWindow> entry : windows.entrySet()) {
            ChunkWindow window = entry.getValue();
            boolean shouldFlush = window.size() >= minEvents
                    || (now - window.getCreatedAt()) >= maxAgeMs;
            if (shouldFlush) {
                ChunkWindow removed = windows.remove(entry.getKey());
                if (removed != null) {
                    result.add(removed);
                }
            }
        }
        return result;
    }

    /**
     * Drains all windows regardless of age/size (used on shutdown).
     */
    public List<ChunkWindow> drainAll() {
        List<ChunkWindow> result = new ArrayList<>(windows.values());
        windows.clear();
        return result;
    }

    @Getter
    public static class ChunkWindow {
        private final String tenantId;
        private final String service;
        private final long windowStart;
        private final long createdAt;
        private final List<LogEvent> events = new ArrayList<>();

        public ChunkWindow(String tenantId, String service, long windowStart) {
            this.tenantId = tenantId;
            this.service = service;
            this.windowStart = windowStart;
            this.createdAt = System.currentTimeMillis();
        }

        public synchronized void addEvent(LogEvent event) {
            events.add(event);
        }

        public synchronized int size() {
            return events.size();
        }

        public synchronized List<LogEvent> getEvents() {
            return new ArrayList<>(events);
        }
    }
}
