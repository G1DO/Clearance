package com.clearance.controller.scheduling;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Authoritative result of a committed runner claim.
 *
 * <p>Identity roles are distinct: {@code jobId} is the logical job, {@code attemptId} the
 * execution attempt created by this claim, {@code allocationId} the ownership binding that
 * allocation to one runner, and {@code runnerEpoch} the runner ownership generation after the
 * monotonic advance performed atomically by the claim. A {@code Claim} instance is only
 * constructed inside the claim transaction and returned after it commits, so it is never
 * exposed as authoritative before PostgreSQL has durably committed it.
 */
public record Claim(
    UUID allocationId,
    UUID attemptId,
    UUID jobId,
    UUID runnerId,
    long runnerEpoch,
    OffsetDateTime createdAt) {}
