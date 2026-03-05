package com.logagg.query.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.logagg.common.model.LogEvent;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
public class QueryResponse {

    @JsonProperty("items")
    private List<LogEvent> items;

    @JsonProperty("stats")
    private QueryStats stats;
}
