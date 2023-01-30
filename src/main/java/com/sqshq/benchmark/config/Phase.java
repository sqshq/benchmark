package com.sqshq.benchmark.config;

import java.util.List;

public record Phase(
    String Duration,
    String Database,
    List<Operation> Operations
) {}