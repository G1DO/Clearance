package com.clearance.controller.agent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.SecureRandom;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * External monotonic source for recovery generations (issue #39).
 *
 * <p>PostgreSQL remains the sole durable ownership authority; this source is never consulted for
 * claim/report gating. It only issues generation values from state outside the rewound database so
 * that restoring an older snapshot cannot cause a repeat.
 *
 * <p>Generations keep the UUID representation but are time-ordered UUIDv7: the leading 48 bits
 * carry the external wall-clock timestamp in milliseconds, so successive generations are strictly
 * newer in value order while the clock advances. Every issued generation is appended to a durable
 * log file outside PostgreSQL (configured via {@code clearance.recovery-generation-log}, default
 * {@code /var/lib/clearance/recovery-generations.log}) with synchronous writes before it is
 * returned. The log file must live on durable persistent storage outside the rewound PostgreSQL
 * state and survive {@code /tmp} clears, host replacement, and container restarts; the operator
 * must provision the directory writable by the controller or override the path to equivalent
 * durable storage. The log is the monotonic memory: issuance reads the
 * logged history, and auto-issuance bumps the timestamp past the logged maximum when the clock
 * has not advanced, so a rewound authority table plus a regressed clock still cannot repeat a
 * value. Operator-supplied generations (see {@code clearance.recovery-generation}) must be
 * UUIDv7 and are fenced: a non-v7 value, a duplicate of logged history, or a UUIDv7 timestamp
 * that does not exceed the logged maximum, is rejected without any database write and the boot
 * fails fast.
 *
 * <p>Read-compute-append is guarded by an exclusive {@link FileChannel} lock on a sibling lock
 * file plus the instance monitor, so two controller processes sharing one log path cannot issue
 * equal timestamps concurrently. For multi-instance deployments the log file must live on shared
 * durable storage, or the operator must supply a single externally-attested UUIDv7 generation to
 * every instance; per-host files cannot coordinate concurrent recovery boots. Recovery itself
 * remains a single operator action.
 */
@Component
public class RecoveryGenerationSource {

  private static final Logger log = LoggerFactory.getLogger(RecoveryGenerationSource.class);

  private static final SecureRandom RANDOM = new SecureRandom();

  /**
   * JVM-wide mutual exclusion for log coordination. Instance monitors ({@code synchronized})
   * serialize threads on one source; this serializes threads across sources sharing one log
   * file in the same JVM, so overlapping NIO file-lock requests from one JVM never surface
   * as {@code OverlappingFileLockException} instead of blocking.
   */
  private static final Object CROSS_INSTANCE_LOCK = new Object();

  private final Path logFile;
  private final Clock clock;

  @Autowired
  public RecoveryGenerationSource(
      @Value("${clearance.recovery-generation-log:/var/lib/clearance/recovery-generations.log}")
      String logFile) {
    this(Path.of(logFile), Clock.systemUTC());
  }

  /** Test-only construction with an isolated log file and clock. */
  public RecoveryGenerationSource(Path logFile, Clock clock) {
    this.logFile = logFile;
    this.clock = clock;
  }

  /** Issues one fresh generation from the external monotonic source and durably logs it. */
  public synchronized UUID issueFreshGeneration() {
    return issue(null);
  }

  /**
   * Issues the operator-supplied generation after fencing it against logged history, or an
   * auto-generated UUIDv7 when {@code explicit} is null.
   *
   * @throws IllegalStateException when the log cannot be read/written or the explicit value is
   *     not strictly fresh
   */
  public synchronized UUID issueFreshGeneration(UUID explicit) {
    return issue(explicit);
  }

  /**
   * Issues one auto-generated UUIDv7 with the durability fence applied atomically with issuance.
   * When {@code authorityPresent} is true, an empty or missing log (evidence of durability loss
   * such as a cleared {@code /tmp}, replaced host, or restarted container) refuses the boot
   * instead of falling back to wall-clock randomness; the caller must then supply an
   * externally-attested UUIDv7. The fence check and the log append share one file lock, so a
   * concurrent issuance or log deletion cannot slip between the check and the write.
   *
   * @throws IllegalStateException when the fence refuses the boot or the log is unwritable
   */
  public synchronized UUID issueAutoFenced(boolean authorityPresent) {
    return withFileLock(() -> {
      List<UUID> history = readHistoryLocked();
      if (authorityPresent && history.isEmpty()) {
        throw new IllegalStateException(
            "refusing recovery boot: recovery authority exists but generation log "
                + logFile
                + " is empty/missing (durable evidence lost); supply an externally-attested "
                + "clearance.recovery-generation UUIDv7 to proceed");
      }
      long maxTimestamp = maxV7Timestamp(history);
      long timestamp = Math.max(clock.millis(), maxTimestamp + 1);
      UUID candidate = newUuidV7(timestamp);
      // Random 74 bits make a repeat vanishingly unlikely, but the log is authoritative:
      // never return a value already issued, even across a database rewind.
      while (history.contains(candidate)) {
        timestamp += 1;
        candidate = newUuidV7(timestamp);
      }
      appendLocked(candidate);
      log.info("Issued recovery generation {} (uuidv7 timestamp {}, log {} entries {})",
          candidate, describeTimestamp(candidate), logFile, history.size() + 1);
      return candidate;
    });
  }

  /** Reads the logged issuance history in issuance order (empty when no log exists yet). */
  public synchronized List<UUID> history() {
    return withFileLock(this::readHistoryLocked);
  }

  /** External log location; included in boot logs and test evidence. */
  public Path logFile() {
    return logFile;
  }

  /**
   * Validates an operator-supplied generation against logged history without appending. The
   * explicit path in {@link RecoveryService} calls this before persisting the authority and
   * appends via {@link #recordIssued} only after the database write succeeds, so a transient
   * database failure never burns the attested value into the log.
   *
   * @throws IllegalStateException when the explicit value is not a strictly fresh UUIDv7
   */
  public synchronized void checkExplicitFresh(UUID explicit) {
    withFileLock(() -> {
      List<UUID> history = readHistoryLocked();
      fenceExplicit(explicit, history, maxV7Timestamp(history));
      return null;
    });
  }

  /**
   * Records an issued generation durably; idempotent when the value is already logged (retry
   * with the same attested value or a second instance converging on one supplied value must
   * not duplicate the log or fail).
   */
  public synchronized void recordIssued(UUID generation) {
    withFileLock(() -> {
      List<UUID> history = readHistoryLocked();
      if (history.contains(generation)) {
        log.info("Recovery generation {} already logged in {} ({} entries), skipping duplicate append",
            generation, logFile, history.size());
        return null;
      }
      appendLocked(generation);
      log.info("Recorded recovery generation {} (uuidv7 timestamp {}, log {} entries {})",
          generation, describeTimestamp(generation), logFile, history.size() + 1);
      return null;
    });
  }

  private UUID issue(UUID explicit) {
    return withFileLock(() -> {
      List<UUID> history = readHistoryLocked();
      long maxTimestamp = maxV7Timestamp(history);
      UUID candidate;
      if (explicit != null) {
        fenceExplicit(explicit, history, maxTimestamp);
        candidate = explicit;
      } else {
        long timestamp = Math.max(clock.millis(), maxTimestamp + 1);
        candidate = newUuidV7(timestamp);
        // Random 74 bits make a repeat vanishingly unlikely, but the log is authoritative:
        // never return a value already issued, even across a database rewind.
        while (history.contains(candidate)) {
          timestamp += 1;
          candidate = newUuidV7(timestamp);
        }
      }
      appendLocked(candidate);
      log.info("Issued recovery generation {} (uuidv7 timestamp {}, log {} entries {})",
          candidate, describeTimestamp(candidate), logFile, history.size() + 1);
      return candidate;
    });
  }

  private void fenceExplicit(UUID explicit, List<UUID> history, long maxTimestamp) {
    if (!isUuidV7(explicit)) {
      throw new IllegalStateException(
          "refusing recovery boot: supplied generation " + explicit + " must be UUIDv7");
    }
    if (history.contains(explicit)) {
      throw new IllegalStateException(
          "refusing recovery boot: supplied generation " + explicit + " was already issued");
    }
    if (maxTimestamp >= 0 && timestampMillis(explicit) <= maxTimestamp) {
      throw new IllegalStateException(
          "refusing recovery boot: supplied generation " + explicit
              + " is not monotonically newer than logged history");
    }
  }

  /**
   * Guards read-compute-append against concurrent controller processes sharing one log path.
   * The instance monitor ({@code synchronized}) covers in-JVM threads; the exclusive file
   * lock covers separate processes. The lock file is a sibling sidecar, never the log itself,
   * so locking never truncates issuance evidence.
   */
  private <T> T withFileLock(Callable<T> action) {
    Path lockFile = Path.of(logFile + ".lock");
    synchronized (CROSS_INSTANCE_LOCK) {
      try {
        Path parent = lockFile.getParent();
        if (parent != null) {
          Files.createDirectories(parent);
        }
        try (FileChannel channel = FileChannel.open(lockFile,
            StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            FileLock ignored = channel.lock()) {
          return action.call();
        }
      } catch (IOException e) {
        throw new UncheckedIOException(
            "cannot coordinate recovery generation log " + logFile, e);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new IllegalStateException("recovery generation issuance failed", e);
      }
    }
  }

  private List<UUID> readHistoryLocked() {
    if (!Files.exists(logFile)) {
      return new ArrayList<>();
    }
    try {
      List<UUID> history = new ArrayList<>();
      for (String line : Files.readAllLines(logFile, StandardCharsets.UTF_8)) {
        String trimmed = line.trim();
        if (!trimmed.isEmpty()) {
          history.add(UUID.fromString(trimmed.split("\\s+")[0]));
        }
      }
      return history;
    } catch (IOException e) {
      throw new UncheckedIOException("cannot read recovery generation log " + logFile, e);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("recovery generation log " + logFile + " is corrupted", e);
    }
  }

  private void appendLocked(UUID generation) {
    try {
      if (logFile.getParent() != null) {
        Files.createDirectories(logFile.getParent());
      }
      Files.writeString(logFile, generation + "\n", StandardCharsets.UTF_8,
          StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.SYNC);
    } catch (IOException e) {
      throw new UncheckedIOException(
          "cannot durably record recovery generation " + generation + " in " + logFile, e);
    }
  }

  private static long maxV7Timestamp(List<UUID> history) {
    long max = -1;
    for (UUID generation : history) {
      if (isUuidV7(generation)) {
        max = Math.max(max, timestampMillis(generation));
      }
    }
    return max;
  }

  public static boolean isUuidV7(UUID generation) {
    return generation.version() == 7;
  }

  /** Extracts the leading 48-bit Unix timestamp in milliseconds from a UUIDv7. */
  public static long timestampMillis(UUID generation) {
    if (!isUuidV7(generation)) {
      throw new IllegalArgumentException("not a UUIDv7: " + generation);
    }
    return generation.getMostSignificantBits() >>> 16;
  }

  static String describeTimestamp(UUID generation) {
    if (!isUuidV7(generation)) {
      return "non-v7";
    }
    return String.valueOf(timestampMillis(generation));
  }

  /** Time-ordered UUIDv7 per RFC 9562: 48-bit timestamp, version 7, RFC 4122 variant. */
  public static UUID newUuidV7(long epochMillis) {
    long randA = RANDOM.nextLong() & 0xFFFL;
    long msb = ((epochMillis & 0xFFFFFFFFFFFFL) << 16) | (7L << 12) | randA;
    long randB = RANDOM.nextLong() & 0x3FFFFFFFFFFFFFFFL;
    long lsb = (2L << 62) | randB;
    return new UUID(msb, lsb);
  }
}
