package com.clearance.controller.jobs;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/** Durable v1 job view. {@code jobId} and {@code operationId} are distinct identities. */
public record JobResponse(
    UUID jobId,
    String projectId,
    String operationId,
    List<String> argv,
    String runnerClass,
    String payloadHash,
    OffsetDateTime createdAt,
    String result,
    boolean cancelRequested) {}
