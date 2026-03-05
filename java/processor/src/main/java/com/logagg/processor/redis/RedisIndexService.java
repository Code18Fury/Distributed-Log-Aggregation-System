package com.logagg.processor.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Writes the Redis index entries used by the query service.
 *
 * Index structure (matches Go redis client exactly):
 *   ZSET  key=idx:{tenantId}:{service}   score=windowStartMs   member=s3Key
 *   HASH  key=meta:{s3Key}               fields: tenant_id, service, window_ms, created_at
 */
@Service
public class RedisIndexService {

    private static final Logger log = LoggerFactory.getLogger(RedisIndexService.class);

    private final StringRedisTemplate redisTemplate;

    public RedisIndexService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void indexChunk(String tenantId, String service, long windowStartMs, String s3Key) {
        String zsetKey = "idx:" + tenantId + ":" + service;

        redisTemplate.opsForZSet().add(zsetKey, s3Key, (double) windowStartMs);

        // Store chunk metadata for debugging/stats
        String metaKey = "meta:" + s3Key;
        Map<String, String> meta = new HashMap<>();
        meta.put("tenant_id", tenantId);
        meta.put("service", service);
        meta.put("window_ms", String.valueOf(windowStartMs));
        meta.put("created_at", String.valueOf(Instant.now().getEpochSecond()));
        redisTemplate.opsForHash().putAll(metaKey, meta);

        log.debug("Indexed chunk zsetKey={} s3Key={}", zsetKey, s3Key);
    }
}
