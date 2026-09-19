package com.clearance.controller.jobs;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

/**
 * Project-scoped idempotent job intake.
 *
 * <p>Transaction boundary: each {@link #submit} call runs in a single database transaction. The
 * critical path is explicit SQL using {@code INSERT ... ON CONFLICT (project_id, operation_id) DO
 * NOTHING RETURNING ...} followed, on conflict, by a {@code SELECT} of the existing row and a
 * payload-hash comparison. PostgreSQL's unique constraint is the sole convergence mechanism: no
 * process-local locks, no unbounded in-memory dedup, no external coordination.
 *
 * <p>Isolation is READ COMMITTED (Spring default). A concurrent loser blocks on the speculative
 * unique insert until the winner commits or rolls back, then observes the winner via SELECT, so all
 * identical submissions converge on one {@code job_id} and conflicting reuse leaves the original
 * row untouched.
 */
@Service
public class JobService {

  // Inspectable critical-path SQL. No ORM hides this boundary.
  static final String INSERT_SQL =
      "INSERT INTO jobs (job_id, project_id, operation_id, argv, runner_class, payload_hash) "
          + "VALUES (?, ?, ?, CAST(? AS jsonb), ?, ?) "
          + "ON CONFLICT (project_id, operation_id) DO NOTHING "
          + "RETURNING job_id, project_id, operation_id, argv, runner_class, payload_hash, created_at";

  static final String SELECT_BY_OPERATION_SQL =
      "SELECT job_id, project_id, operation_id, argv, runner_class, payload_hash, created_at "
          + "FROM jobs WHERE project_id = ? AND operation_id = ?";

  static final String SELECT_BY_JOB_ID_SQL =
      "SELECT job_id, project_id, operation_id, argv, runner_class, payload_hash, created_at "
          + "FROM jobs WHERE job_id = ?";

  private final JdbcTemplate jdbc;
  private final ObjectMapper objectMapper;

  public JobService(JdbcTemplate jdbc, ObjectMapper objectMapper) {
    this.jdbc = jdbc;
    this.objectMapper = objectMapper;
  }

  public record SubmitResult(Job job, boolean created) {}

  /**
   * Durably submit or deduplicate a v1 job.
   *
   * @return the canonical job row and whether this call created it
   * @throws IdempotencyConflictException if the operation identity already maps to a different
   *     canonical payload
   */
  @Transactional
  public SubmitResult submit(
      String projectId, String operationId, List<String> argv, String runnerClass) {
    String canonical = Canonicalization.canonicalize(argv, runnerClass);
    String payloadHash = Canonicalization.sha256Hex(canonical);
    String argvJson = toArgvJson(argv);

    // First attempt with a fresh logical job identity.
    List<Job> inserted =
        jdbc.query(INSERT_SQL, rowMapper(), UUID.randomUUID(), projectId, operationId, argvJson,
            runnerClass, payloadHash);
    if (!inserted.isEmpty()) {
      return new SubmitResult(inserted.get(0), true);
    }

    Optional<Job> existing = findByOperation(projectId, operationId);
    if (existing.isEmpty()) {
      // Winner rolled back between our INSERT and SELECT; bounded single retry.
      List<Job> retried =
          jdbc.query(INSERT_SQL, rowMapper(), UUID.randomUUID(), projectId, operationId, argvJson,
              runnerClass, payloadHash);
      if (!retried.isEmpty()) {
        return new SubmitResult(retried.get(0), true);
      }
      existing = findByOperation(projectId, operationId);
    }
    Job current =
        existing.orElseThrow(() -> new IllegalStateException("job missing after idempotent insert"));
    if (!current.payloadHash().equals(payloadHash)) {
      throw new IdempotencyConflictException(current);
    }
    return new SubmitResult(current, false);
  }

  @Transactional(readOnly = true)
  public Job getForProject(UUID jobId, String projectId) {
    List<Job> rows = jdbc.query(SELECT_BY_JOB_ID_SQL, rowMapper(), jobId);
    if (rows.isEmpty()) {
      throw new JobNotFoundException();
    }
    Job job = rows.get(0);
    if (!job.projectId().equals(projectId)) {
      // Cross-project reads are indistinguishable from missing (no oracle).
      throw new JobNotFoundException();
    }
    return job;
  }

  @Transactional(readOnly = true)
  public Optional<Job> findByOperation(String projectId, String operationId) {
    List<Job> rows = jdbc.query(SELECT_BY_OPERATION_SQL, rowMapper(), projectId, operationId);
    return rows.stream().findFirst();
  }

  private String toArgvJson(List<String> argv) {
    try {
      return objectMapper.writeValueAsString(argv);
    } catch (Exception e) {
      throw new IllegalStateException("failed to encode argv", e);
    }
  }

  private RowMapper<Job> rowMapper() {
    return (ResultSet rs, int rowNum) -> mapRow(rs);
  }

  private Job mapRow(ResultSet rs) throws SQLException {
    UUID jobId = (UUID) rs.getObject("job_id");
    String projectId = rs.getString("project_id");
    String operationId = rs.getString("operation_id");
    String argvJson = rs.getString("argv");
    List<String> argv;
    try {
      argv = objectMapper.readValue(argvJson, new TypeReference<List<String>>() {});
    } catch (Exception e) {
      throw new SQLException("failed to decode argv JSONB", e);
    }
    String runnerClass = rs.getString("runner_class");
    String payloadHash = rs.getString("payload_hash");
    OffsetDateTime createdAt = rs.getObject("created_at", OffsetDateTime.class);
    return new Job(jobId, projectId, operationId, argv, runnerClass, payloadHash, createdAt);
  }
}
