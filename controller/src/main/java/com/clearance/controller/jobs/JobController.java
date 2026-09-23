package com.clearance.controller.jobs;

import com.clearance.controller.auth.ApiKeyAuthFilter;
import jakarta.validation.Valid;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * v1 job intake API.
 *
 * <p>{@code project_id} comes from the authenticated API-key scope ({@link ApiKeyAuthFilter}),
 * never from the body. {@code Idempotency-Key} is the v1 {@code operation_id} scoped by that
 * project. First submission creates (201); identical replay returns the same job (200); different
 * canonical payload under the same operation yields 409 without mutating the original.
 */
@RestController
@RequestMapping("/api/v1/jobs")
public class JobController {

  private final JobService service;

  public JobController(JobService service) {
    this.service = service;
  }

  @PostMapping(consumes = "application/json", produces = "application/json")
  public ResponseEntity<JobResponse> submit(
      @RequestAttribute(ApiKeyAuthFilter.PROJECT_ATTRIBUTE) String projectId,
      @RequestHeader("Idempotency-Key") String operationIdRaw,
      @Valid @RequestBody JobRequest body) {
    if (operationIdRaw == null) {
      throw new BadRequestException("missing Idempotency-Key");
    }
    String operationId = operationIdRaw.trim();
    if (operationId.isEmpty()) {
      throw new BadRequestException("Idempotency-Key must be non-empty");
    }
    if (operationId.length() > 128) {
      throw new BadRequestException("Idempotency-Key must be at most 128 characters");
    }
    JobService.SubmitResult result =
        service.submit(projectId, operationId, body.argv(), body.runnerClass());
    JobResponse response = toResponse(result.job());
    if (result.created()) {
      return ResponseEntity.status(201).body(response);
    }
    return ResponseEntity.ok(response);
  }

  @GetMapping(value = "/{jobId}", produces = "application/json")
  public ResponseEntity<JobResponse> get(
      @RequestAttribute(ApiKeyAuthFilter.PROJECT_ATTRIBUTE) String projectId,
      @PathVariable UUID jobId) {
    Job job = service.getForProject(jobId, projectId);
    return ResponseEntity.ok(toResponse(job));
  }

  @PostMapping(value = "/{jobId}/cancel", produces = "application/json")
  public ResponseEntity<JobResponse> cancel(
      @RequestAttribute(ApiKeyAuthFilter.PROJECT_ATTRIBUTE) String projectId,
      @PathVariable UUID jobId) {
    return ResponseEntity.ok(toResponse(service.cancelForProject(jobId, projectId)));
  }

  private static JobResponse toResponse(Job job) {
    return new JobResponse(
        job.jobId(),
        job.projectId(),
        job.operationId(),
        job.argv(),
        job.runnerClass(),
        job.payloadHash(),
        job.createdAt(),
        job.result(),
        job.cancelRequested());
  }
}
