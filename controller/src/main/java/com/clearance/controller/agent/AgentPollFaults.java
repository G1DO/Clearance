package com.clearance.controller.agent;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/** One-shot transport faults scoped to authenticated runners, available only in the test profile. */
@Component
@Profile("test")
public class AgentPollFaults {

  private record PollFault(boolean drop, long delayMs) {}

  private final ConcurrentMap<UUID, PollFault> polls = new ConcurrentHashMap<>();
  private final ConcurrentMap<UUID, Long> reports = new ConcurrentHashMap<>();

  public void configure(UUID runnerId, boolean dropPoll, long pollDelayMs, long reportDelayMs) {
    if (dropPoll || pollDelayMs > 0) {
      polls.put(runnerId, new PollFault(dropPoll, pollDelayMs));
    } else {
      polls.remove(runnerId);
    }
    if (reportDelayMs > 0) {
      reports.put(runnerId, reportDelayMs);
    } else {
      reports.remove(runnerId);
    }
  }

  /** Called only after an assigned poll has committed; null suppresses its allocation body. */
  public String deliverPoll(UUID runnerId, String serializedPoll) {
    PollFault fault = polls.remove(runnerId);
    if (fault == null) {
      return serializedPoll;
    }
    delay(fault.delayMs());
    return fault.drop() ? null : serializedPoll;
  }

  /** Delay before report ingestion opens its database transaction. */
  public void beforeReport(UUID runnerId) {
    Long delayMs = reports.remove(runnerId);
    if (delayMs != null) {
      delay(delayMs);
    }
  }

  private static void delay(long milliseconds) {
    if (milliseconds <= 0) {
      return;
    }
    try {
      Thread.sleep(milliseconds);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("test fault delay interrupted", e);
    }
  }
}
