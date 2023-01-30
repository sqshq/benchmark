package com.sqshq.benchmark.config;

public record Operation(
    String OperationMetricsName,
    String OperationName,
    Command OperationCommand
) {}