package com.logagg.common.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.logagg.common.model.LogEvent;

import java.io.*;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Utility methods for chunking, serialization, and compression.
 * These replicate the behavior of the Go chunker package exactly.
 */
public final class ChunkUtils {

    public static final long WINDOW_SIZE_MS = 5L * 60 * 1000; // 5 minutes
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ChunkUtils() {}

    /**
     * Returns the window start for a timestamp, aligned to epoch boundaries.
     * Equivalent to Go: (timestampMs / windowSizeMs) * windowSizeMs
     */
    public static long getWindowStart(long timestampMs) {
        return (timestampMs / WINDOW_SIZE_MS) * WINDOW_SIZE_MS;
    }

    /**
     * Generates an S3 key for a chunk.
     * Format: tenant/{tenant}/service/{service}/yyyy={YYYY}/mm={MM}/dd={DD}/hh={HH}/min={MIN}/chunk_{uuid}.json.gz
     */
    public static String generateS3Key(String tenantId, String service, long windowStartMs) {
        ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(windowStartMs), ZoneOffset.UTC);
        return String.format(
            "tenant/%s/service/%s/yyyy=%04d/mm=%02d/dd=%02d/hh=%02d/min=%02d/chunk_%s.json.gz",
            tenantId, service,
            zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth(),
            zdt.getHour(), zdt.getMinute(),
            UUID.randomUUID()
        );
    }

    /**
     * Serializes a list of LogEvents to newline-delimited JSON (NDJSON).
     * Each line is one JSON-serialized LogEvent.
     */
    public static byte[] serializeToNdjson(List<LogEvent> events) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (LogEvent event : events) {
            baos.write(MAPPER.writeValueAsBytes(event));
            baos.write('\n');
        }
        return baos.toByteArray();
    }

    /**
     * Deserializes NDJSON bytes back to a list of LogEvents.
     */
    public static List<LogEvent> deserializeFromNdjson(byte[] data) throws IOException {
        List<LogEvent> events = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new ByteArrayInputStream(data)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    events.add(MAPPER.readValue(line, LogEvent.class));
                }
            }
        }
        return events;
    }

    /**
     * Compresses data using gzip at best compression level.
     * Equivalent to Go: gzip.NewWriterLevel(&buf, gzip.BestCompression)
     */
    public static byte[] compress(byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        // Anonymous subclass to set compression level before first write
        GZIPOutputStream gzip = new GZIPOutputStream(baos) {
            { def.setLevel(Deflater.BEST_COMPRESSION); }
        };
        try {
            gzip.write(data);
        } finally {
            gzip.close();
        }
        return baos.toByteArray();
    }

    /**
     * Decompresses gzip-compressed data.
     */
    public static byte[] decompress(byte[] compressed) throws IOException {
        try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressed));
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int len;
            while ((len = gzip.read(buffer)) != -1) {
                baos.write(buffer, 0, len);
            }
            return baos.toByteArray();
        }
    }
}
