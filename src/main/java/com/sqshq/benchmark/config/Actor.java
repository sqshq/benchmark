package com.sqshq.benchmark.config;

import java.util.List;

public record Actor(
    String Name,
    String Type,
    int Threads,
    List<Phase> Phases
) {}
