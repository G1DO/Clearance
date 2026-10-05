package com.clearance.controller.agent;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Boots the controller in recovery mode after a PostgreSQL restore when enabled.
 *
 * <p>When {@code clearance.recovery-mode} is true, issues one fresh recovery generation from the
 * external monotonic source (time-ordered UUIDv7 plus a durable log outside the rewound database;
 * see {@link RecoveryGenerationSource} and {@link RecoveryService}) and holds the fleet
 * quarantined without scheduling. When {@code clearance.recovery-generation} supplies an
 * externally-attested UUIDv7, the boot is fenced on it: a non-v7, duplicate, or non-monotonic
 * value, or an unwritable generation log, fails the boot fast before any authority write instead
 * of issuing a repeat. A boot with authority present but an empty/missing log is likewise refused
 * unless such an attested value is supplied. Otherwise the boot preserves the existing authority
 * untouched. Flyway migrations run before this runner, so the authority table always exists.
 */
@Component
public class RecoveryBootRunner implements ApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(RecoveryBootRunner.class);

  private final RecoveryService recovery;
  private final boolean recoveryMode;
  private final String explicitGeneration;

  public RecoveryBootRunner(
      RecoveryService recovery,
      @Value("${clearance.recovery-mode:false}") boolean recoveryMode,
      @Value("${clearance.recovery-generation:}") String explicitGeneration) {
    this.recovery = recovery;
    this.recoveryMode = recoveryMode;
    this.explicitGeneration = explicitGeneration == null ? "" : explicitGeneration.trim();
  }

  @Override
  public void run(ApplicationArguments args) {
    if (!recoveryMode) {
      recovery.currentGeneration().ifPresentOrElse(
          generation -> log.info("Normal boot: preserving current recovery generation {}", generation),
          () -> log.info("Normal boot: no recovery authority present, scheduling uses legacy availability"));
      return;
    }
    log.info("Recovery-mode boot requested: establishing fresh generation and quarantining fleet");
    if (!explicitGeneration.isEmpty()) {
      UUID supplied;
      try {
        supplied = UUID.fromString(explicitGeneration);
      } catch (IllegalArgumentException e) {
        throw new IllegalStateException(
            "refusing recovery boot: clearance.recovery-generation is not a valid UUID", e);
      }
      log.info("Recovery-mode boot using operator-supplied externally-attested generation {}",
          supplied);
      recovery.enterRecoveryMode(supplied);
      return;
    }
    recovery.enterRecoveryMode();
  }
}
