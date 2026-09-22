package com.clearance.controller.agent;

import com.clearance.controller.agent.AgentProtocol.PollResponse;
import com.clearance.controller.agent.AgentProtocol.ReportRequest;
import com.clearance.controller.agent.AgentProtocol.ReportResponse;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

/** PostgreSQL is the sole authority. All paths lock runner, then allocation, like claim. */
@Service
public class AgentService {
  private final JdbcTemplate jdbc;
  private final ObjectMapper mapper;

  public AgentService(JdbcTemplate jdbc, ObjectMapper mapper) {
    this.jdbc = jdbc;
    this.mapper = mapper;
  }

  private record Runner(long epoch, Long incarnation, String state) {}
  private record Allocation(UUID id, UUID runnerId, long epoch, Long incarnation,
                            long maxSeq, String status) {}

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
        SELECT a.allocation_id, a.job_id, a.runner_epoch, j.argv::text, j.runner_class
        FROM allocations a JOIN jobs j ON j.job_id = a.job_id
        WHERE a.runner_id = ? AND a.state = 'ACTIVE' FOR UPDATE OF a
        """, (rs, n) -> new PollResponse(true, rs.getObject("allocation_id", UUID.class),
            rs.getObject("job_id", UUID.class), rs.getLong("runner_epoch"),
            List.of(mapper.readValue(rs.getString("argv"), String[].class)),
            rs.getString("runner_class"), null), runnerId);
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
        SELECT allocation_id, runner_id, runner_epoch, agent_incarnation, max_seq, report_status
        FROM allocations WHERE allocation_id = ? AND state = 'ACTIVE' FOR UPDATE
        """, (rs, n) -> new Allocation(rs.getObject("allocation_id", UUID.class),
            rs.getObject("runner_id", UUID.class), rs.getLong("runner_epoch"),
            rs.getObject("agent_incarnation", Long.class), rs.getLong("max_seq"),
            rs.getString("report_status")), report.allocationId());
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
        || !Objects.equals(allocation.incarnation(), report.agentIncarnation())
        || "AVAILABLE".equals(runner.state())) {
      return new ReportResponse(false, "fenced_rejected", terminal);
    }
    if (report.seq() <= allocation.maxSeq()) {
      return new ReportResponse(false, "dropped_stale", terminal);
    }
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
    // Reports record observations only: runner state/epoch and ACTIVE ownership never change.
    // In particular, neither terminal success/failure nor heartbeat can release/quarantine.
    return new ReportResponse(true, "ok", isTerminal(nextStatus));
  }

  private static boolean isTerminal(String status) {
    return "SUCCEEDED".equals(status) || "FAILED".equals(status);
  }
}
