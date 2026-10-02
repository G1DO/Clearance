package com.clearance.controller.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Boots the controller in recovery mode after a PostgreSQL restore when enabled.
 *
 * <p>When {@code clearance.recovery-mode} is true, issues one fresh recovery generation and holds
 * the fleet quarantined without scheduling (see {@link RecoveryService}). Otherwise the boot
 * preserves the existing authority untouched. Flyway migrations run before this runner, so the
 * authority table always exists.
 */
@Component
public class RecoveryBootRunner implements ApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(RecoveryBootRunner.class);

  private final RecoveryService recovery;
  private final boolean recoveryMode;

  public RecoveryBootRunner(
      RecoveryService recovery,
      @Value("${clearance.recovery-mode:false}") boolean recoveryMode) {
    this.recovery = recovery;
    this.recoveryMode = recoveryMode;
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
    recovery.enterRecoveryMode();
  }
}
