package com.logagg.query.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class QueryStats {

    @JsonProperty("objects_read")
    private int objectsRead;

    @JsonProperty("events_scanned")
    private int eventsScanned;

    @JsonProperty("events_returned")
    private int eventsReturned;

    @JsonProperty("duration_ms")
    private long durationMs;
}
