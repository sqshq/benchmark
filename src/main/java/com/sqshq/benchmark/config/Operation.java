package com.sqshq.benchmark.config;

public record Operation(
    String OperationName,
    Command OperationCommand
) {}