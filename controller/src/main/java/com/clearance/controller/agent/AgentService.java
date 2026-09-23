package com.clearance.controller.agent;

import com.clearance.controller.agent.AgentProtocol.PollResponse;
import com.clearance.controller.agent.AgentProtocol.ReportRequest;
import com.clearance.controller.agent.AgentProtocol.ReportResponse;
import com.clearance.controller.scheduling.SchedulerService;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

/** PostgreSQL is the sole authority. All ownership paths lock the runner first. */
@Service
public class AgentService {
  private final JdbcTemplate jdbc;
  private final ObjectMapper mapper;
  private final SchedulerService scheduler;

  public AgentService(JdbcTemplate jdbc, ObjectMapper mapper, SchedulerService scheduler) {
    this.jdbc = jdbc;
    this.mapper = mapper;
    this.scheduler = scheduler;
  }

  private record Runner(long epoch, Long incarnation, String state) {}
  private record Allocation(UUID id, UUID runnerId, UUID jobId, UUID attemptId,
                            long epoch, Long incarnation, long maxSeq, String status, String state,
                            boolean discoveryRequired, Long recoveryIncarnation) {}

  private Runner lockRunner(UUID runnerId) {
    var runners = jdbc.query("""
        SELECT epoch, agent_incarnation, state FROM runners WHERE runner_id = ? FOR UPDATE
        """, (rs, n) -> new Runner(rs.getLong("epoch"),
            rs.getObject("agent_incarnation", Long.class), rs.getString("state")), runnerId);
    if (runners.isEmpty()) {
      throw new AgentApiException(403, "fenced_rejected", "machine identity has no seeded runner");
    }
    return runners.getFirst();
  }

  /** Each observation commits before delivery; the long-poll wait holds no transaction. */
  @Transactional(isolation = Isolation.READ_COMMITTED)
  public PollResponse poll(UUID runnerId, long incarnation) {
    Runner runner = lockRunner(runnerId);
    if (runner.incarnation() != null && incarnation < runner.incarnation()) {
      throw new AgentApiException(409, "fenced_rejected", "agent incarnation has been superseded");
    }
    var assignments = jdbc.query("""
        SELECT a.allocation_id, a.job_id, a.runner_epoch, j.argv::text, j.runner_class,
               j.cancel_requested, a.workload_timeout_ms
        FROM allocations a JOIN jobs j ON j.job_id = a.job_id
        WHERE a.runner_id = ? AND a.state = 'ACTIVE' FOR UPDATE OF a
        """, (rs, n) -> new PollResponse(true, rs.getObject("allocation_id", UUID.class),
            rs.getObject("job_id", UUID.class), rs.getLong("runner_epoch"),
            List.of(mapper.readValue(rs.getString("argv"), String[].class)),
            rs.getString("runner_class"), null, rs.getBoolean("cancel_requested"),
            rs.getLong("workload_timeout_ms")), runnerId);
    PollResponse response = assignments.isEmpty()
        ? new PollResponse(false, null, null, null, null, null, null)
        : assignments.getFirst();
    if (response.assigned()
        && (response.runnerEpoch() != runner.epoch() || "AVAILABLE".equals(runner.state()))) {
      throw new AgentApiException(409, "fenced_rejected", "allocation is not current ownership");
    }
    // Equal-incarnation re-polls do not touch even updated_at/xmin. A delayed old poll
    // cannot rebind a superseded process. Neither rotation nor delivery resets max_seq.
    if (!Objects.equals(runner.incarnation(), incarnation)) {
      jdbc.update("""
          UPDATE runners SET agent_incarnation = ?, updated_at = now() WHERE runner_id = ?
          """, incarnation, runnerId);
    }
    if (response.assigned()) {
      jdbc.update("""
          UPDATE allocations SET agent_incarnation = ?
          WHERE allocation_id = ? AND agent_incarnation IS DISTINCT FROM ?
          """, incarnation, response.allocationId(), incarnation);
    }
    return response;
  }

  /** No write occurs until identity, fencing, sequence, and sticky-result checks all pass. */
  @Transactional(isolation = Isolation.READ_COMMITTED)
  public ReportResponse report(UUID runnerId, ReportRequest report) {
    Runner runner = lockRunner(runnerId);
    var allocations = jdbc.query("""
        SELECT allocation_id, runner_id, job_id, attempt_id, runner_epoch,
               agent_incarnation, max_seq, report_status, state, discovery_required, recovery_incarnation
        FROM allocations WHERE allocation_id = ? FOR UPDATE
        """, (rs, n) -> new Allocation(rs.getObject("allocation_id", UUID.class),
            rs.getObject("runner_id", UUID.class), rs.getObject("job_id", UUID.class),
            rs.getObject("attempt_id", UUID.class), rs.getLong("runner_epoch"),
            rs.getObject("agent_incarnation", Long.class), rs.getLong("max_seq"),
            rs.getString("report_status"), rs.getString("state"), rs.getBoolean("discovery_required"),
            rs.getObject("recovery_incarnation", Long.class)), report.allocationId());
    if (allocations.isEmpty()) {
      return new ReportResponse(false, "fenced_rejected", false);
    }
    Allocation allocation = allocations.getFirst();
    if (!runnerId.equals(allocation.runnerId())) {
      throw new AgentApiException(403, "fenced_rejected", "allocation belongs to another runner");
    }
    boolean terminal = isTerminal(allocation.status());
    if (report.runnerEpoch() != runner.epoch() || report.runnerEpoch() != allocation.epoch()
        || !Objects.equals(runner.incarnation(), report.agentIncarnation())
        || !Objects.equals(allocation.incarnation(), report.agentIncarnation())) {
      return new ReportResponse(false, "fenced_rejected", terminal);
    }
    boolean cleanup = report.status() == AgentProtocol.ReportStatus.CLEANUP;
    if ("RELEASED".equals(allocation.state())) {
      // A lost cleanup acknowledgment can be retried, including its exact sequence,
      // only while the same ownership generation/incarnation is still current.
      if (cleanup && "AVAILABLE".equals(runner.state()) && positive(report.cleanup())
          && report.seq() >= allocation.maxSeq()) return new ReportResponse(true, "ok", terminal);
      return new ReportResponse(false, "fenced_rejected", terminal);
    }
    if ("AVAILABLE".equals(runner.state())) {
      return new ReportResponse(false, "fenced_rejected", terminal);
    }
    if (report.seq() <= allocation.maxSeq()) {
      return new ReportResponse(false, "dropped_stale", terminal);
    }
    if (report.status() == AgentProtocol.ReportStatus.RECOVERY) {
      return recover(runnerId, runner, allocation, report, terminal);
    }
    if (cleanup) return cleanup(runnerId, runner, allocation, report, terminal);
    String status = report.status().name();
    boolean heartbeat = report.status() == AgentProtocol.ReportStatus.HEARTBEAT;
    if (terminal && !heartbeat && !status.equals(allocation.status())) {
      return new ReportResponse(false, "terminal_sticky", true);
    }
    // Progress is monotonic even if the sender gives STARTING a newer sequence after RUNNING.
    String nextStatus = heartbeat || ("RUNNING".equals(allocation.status()) && "STARTING".equals(status))
        ? allocation.status() : status;
    jdbc.update("""
        UPDATE allocations SET max_seq = ?, report_status = ? WHERE allocation_id = ?
        """, report.seq(), nextStatus, allocation.id());
    if (!terminal && isTerminal(nextStatus)) {
      jdbc.update("UPDATE attempts SET result = ? WHERE attempt_id = ? AND result IS NULL",
          nextStatus, allocation.attemptId());
      jdbc.update("UPDATE jobs SET result = ? WHERE job_id = ? AND result IS NULL",
          nextStatus, allocation.jobId());
      // Terminal execution is not safe-reuse proof. Existing quarantine is sticky.
      jdbc.update("""
          UPDATE runners SET state = 'CLEANING', updated_at = now()
          WHERE runner_id = ? AND state <> 'QUARANTINED'
          """, runnerId);
    }
    return new ReportResponse(true, "ok", isTerminal(nextStatus));
  }

  private ReportResponse recover(UUID runnerId, Runner runner, Allocation allocation,
      ReportRequest report, boolean terminal) {
    if (report.discovery() == null) return new ReportResponse(false, "discovery_required", terminal);
    if ("QUARANTINED".equals(runner.state())) return new ReportResponse(false, "quarantined", terminal);
    var evidence = report.discovery();
    var fields = new java.util.LinkedHashMap<String, Object>();
    fields.put("agent_incarnation", report.agentIncarnation());
    fields.put("seq", report.seq());
    fields.put("cgroup_present", evidence.cgroupPresent());
    fields.put("workspace_present", evidence.workspacePresent());
    fields.put("cleanup_verified", evidence.cleanupVerified());
    fields.put("pids", evidence.pids());
    if (evidence.error() != null) fields.put("error", evidence.error());
    String encoded = mapper.writeValueAsString(fields);
    // Absent resources require a durable removal checkpoint independently verified
    // by the current agent against Linux state; a launch or terminal marker is insufficient.
    boolean resolved = evidence.error() == null
        && (evidence.cgroupPresent() && evidence.workspacePresent() || evidence.cleanupVerified())
        && (!evidence.cleanupVerified() || evidence.pids().isEmpty());
    String action = resolved ? "TERMINATE" : "QUARANTINE";
    jdbc.update("""
        UPDATE allocations SET max_seq = ?, discovery_required = true, recovery_action = ?,
          recovery_evidence = CAST(? AS jsonb), recovery_incarnation = ?,
          report_status = CASE WHEN ? THEN 'INTERRUPTED' ELSE report_status END
        WHERE allocation_id = ?
        """, report.seq(), action, encoded, report.agentIncarnation(), resolved && !terminal, allocation.id());
    if (!resolved) {
      jdbc.update("""
          UPDATE runners SET state = 'QUARANTINED', quarantine_reason = ?, updated_at = now()
          WHERE runner_id = ?
          """, "recovery discovery unresolved: " + encoded, runnerId);
      return new ReportResponse(true, "quarantined", terminal);
    }
    if (!terminal) {
      jdbc.update("""
          UPDATE attempts SET result = 'INTERRUPTED', recovery_reason = 'agent_restart_without_resume_proof'
          WHERE attempt_id = ? AND result IS NULL
          """, allocation.attemptId());
    }
    // No generic resumption proof exists. The attempt is interrupted, but its
    // allocation remains owned until current physical cleanup proof is accepted.
    jdbc.update("UPDATE runners SET state = 'CLEANING', updated_at = now() WHERE runner_id = ?", runnerId);
    return new ReportResponse(true, "terminate", true);
  }

  private ReportResponse cleanup(UUID runnerId, Runner runner, Allocation allocation,
      ReportRequest report, boolean terminal) {
    if (!terminal) return new ReportResponse(false, "terminal_required", false);
    if (allocation.discoveryRequired()
        && !Objects.equals(allocation.recoveryIncarnation(), report.agentIncarnation())) {
      return new ReportResponse(false, "recovery_required", true);
    }
    if (report.cleanup() == null) return new ReportResponse(false, "cleanup_required", true);
    if ("QUARANTINED".equals(runner.state())) return new ReportResponse(false, "quarantined", true);
    var evidence = report.cleanup();
    var fields = new java.util.LinkedHashMap<String, Object>();
    fields.put("execution_empty", evidence.executionEmpty());
    fields.put("descendants_reaped", evidence.descendantsReaped());
    fields.put("workspace_clean", evidence.workspaceClean());
    if (evidence.error() != null) fields.put("error", evidence.error());
    String encoded = mapper.writeValueAsString(fields);
    if (!positive(evidence)) {
      jdbc.update("""
          UPDATE allocations SET max_seq = ?, cleanup_evidence = CAST(? AS jsonb) WHERE allocation_id = ?
          """, report.seq(), encoded, allocation.id());
      jdbc.update("""
          UPDATE runners SET state = 'QUARANTINED', quarantine_reason = ?, updated_at = now()
          WHERE runner_id = ?
          """, "cleanup failed: " + encoded, runnerId);
      return new ReportResponse(true, "quarantined", true);
    }
    // Both writes become visible together; the held runner lock serializes next claim.
    jdbc.update("""
        UPDATE allocations SET state = 'RELEASED', max_seq = ?, cleanup_evidence = CAST(? AS jsonb),
          released_at = now() WHERE allocation_id = ?
        """, report.seq(), encoded, allocation.id());
    jdbc.update("UPDATE runners SET state = 'AVAILABLE', updated_at = now() WHERE runner_id = ?", runnerId);
    if ("INTERRUPTED".equals(allocation.status())) {
      // Reuse the ordinary claim transaction, under the same runner lock. No
      // retry is committed or delivered until execution is positively stopped,
      // workspace cleanup is proven, and old ownership is released atomically.
      var retry = scheduler.claim(allocation.jobId(), runnerId);
      if (retry.isPresent()) {
        jdbc.update("UPDATE allocations SET retry_allocation_id = ? WHERE allocation_id = ?",
            retry.get().allocationId(), allocation.id());
      } else {
        jdbc.update("""
            UPDATE jobs SET result = 'CANCELLED'
            WHERE job_id = ? AND result IS NULL AND cancel_requested
            """, allocation.jobId());
      }
    }
    return new ReportResponse(true, "ok", true);
  }

  private static boolean positive(AgentProtocol.CleanupEvidence evidence) {
    return evidence != null && evidence.executionEmpty() && evidence.descendantsReaped()
        && evidence.workspaceClean() && evidence.error() == null;
  }

  private static boolean isTerminal(String status) {
    return "SUCCEEDED".equals(status) || "FAILED".equals(status)
        || "CANCELLED".equals(status) || "TIMED_OUT".equals(status) || "INTERRUPTED".equals(status);
  }
}
