package com.clearance.controller.jobs;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/** Internal durable job row. */
public record Job(
    UUID jobId,
    String projectId,
    String operationId,
    List<String> argv,
    String runnerClass,
    String payloadHash,
    OffsetDateTime createdAt,
    String result,
    boolean cancelRequested) {}
