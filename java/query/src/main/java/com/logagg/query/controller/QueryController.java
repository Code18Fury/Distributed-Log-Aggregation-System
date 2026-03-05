package com.logagg.query.controller;

import com.logagg.query.model.QueryRequest;
import com.logagg.query.model.QueryResponse;
import com.logagg.query.service.QueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST controller for log queries.
 * Matches the Go query service API contract exactly.
 */
@RestController
public class QueryController {

    private static final Logger log = LoggerFactory.getLogger(QueryController.class);

    private final QueryService queryService;

    public QueryController(QueryService queryService) {
        this.queryService = queryService;
    }

    @PostMapping("/v1/query")
    public ResponseEntity<?> query(@RequestBody QueryRequest req) {
        if (req.getTenantId() == null || req.getTenantId().isBlank()) {
            return ResponseEntity.badRequest().body("tenant_id is required");
        }
        if (req.getFromTs() <= 0 || req.getToTs() <= 0) {
            return ResponseEntity.badRequest().body("from_ts and to_ts are required");
        }
        if (req.getService() == null || req.getService().isBlank()) {
            return ResponseEntity.badRequest().body("service filter is required in MVP");
        }

        log.info("Query request tenant={} service={} from={} to={}",
                req.getTenantId(), req.getService(), req.getFromTs(), req.getToTs());

        try {
            QueryResponse response = queryService.query(req);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Query failed", e);
            return ResponseEntity.internalServerError().body("Query failed: " + e.getMessage());
        }
    }

    @GetMapping("/healthz")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "healthy"));
    }

    @GetMapping("/readyz")
    public ResponseEntity<Map<String, String>> ready() {
        return ResponseEntity.ok(Map.of("status", "ready"));
    }
}
