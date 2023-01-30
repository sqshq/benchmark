package com.sqshq.benchmark.config;

import com.fasterxml.jackson.databind.JsonNode;

public record Command(
    String aggregate,
    JsonNode pipeline
) {}