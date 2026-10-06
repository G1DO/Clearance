import com.clearance.controller.Application;
import com.clearance.controller.auth.AuthProperties;
import com.clearance.controller.agent.ReconciliationService;
import com.clearance.controller.agent.RecoveryService;
import com.clearance.controller.jobs.JobService;
import com.clearance.controller.scheduling.SchedulerService;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.ObjectMapper;

/** Test-only launcher: the Go suite drives the actual controller and scheduler on PostgreSQL. */
public class ControllerHarness {
  private static final ObjectMapper JSON = new ObjectMapper();

  /** Credentials stashed outside Spring so a stopped controller can be replaced. */
  private record SavedAuth(Map<String, UUID> runnerKeys, Map<String, String> apiKeys) {}
  private static final String WORKLOAD = """
      mkdir -p nested/deeper
      printf 'workspace data\\n' > nested/deeper/file
      echo $$ > "$1/direct.pid"
      /bin/sh -c 'echo $$ > "$1/child.pid"; sleep 60 & echo $! > "$1/grandchild.pid"; wait' child "$1" &
      /bin/sh -c 'setsid /bin/sh -c "$2" orphan "$1" </dev/null >/dev/null 2>&1 &' detach "$1" 'trap "" TERM; echo $$ > "$1/orphan.pid"; while :; do sleep 1; done'
      while [ ! -s "$1/child.pid" ] || [ ! -s "$1/grandchild.pid" ] || [ ! -s "$1/orphan.pid" ]; do sleep 0.01; done
      printf 'ready\\n' > "$1/ready"
      while [ ! -e "$1/finish" ]; do sleep 0.02; done
      exit "$2"
      """;

  private static ConfigurableApplicationContext start(String schema, int port) {
    return start(schema, port, false);
  }

  private static ConfigurableApplicationContext start(String schema, int port, boolean recoveryMode) {
    return new SpringApplicationBuilder(Application.class).web(WebApplicationType.SERVLET)
        .run(controllerArgs(schema, port, recoveryMode).toArray(new String[0]));
  }

  private static String jdbcUrl(String schema) {
    return "jdbc:postgresql://" + System.getenv("PGHOST") + ":"
        + System.getenv("PGPORT") + "/" + System.getenv("PGDATABASE")
        + "?currentSchema=" + schema;
  }

  private static String generationLogPath() {
    // Pin the generation log to test scratch: the production default is a
    // persistent host path (/var/lib/clearance) that tests must not touch.
    return System.getProperty("java.io.tmpdir") + "/clearance-recovery-generations-go-test.log";
  }

  private static List<String> controllerArgs(String schema, int port, boolean recoveryMode) {
    return List.of("--server.port=" + port, "--spring.profiles.active=test",
        "--spring.datasource.url=" + jdbcUrl(schema),
        "--spring.datasource.username=" + System.getenv("PGUSER"),
        "--spring.datasource.password=" + System.getenv("PGPASSWORD"),
        "--spring.flyway.schemas=" + schema,
        "--spring.flyway.default-schema=" + schema,
        "--clearance.heartbeat-timeout-ms=120000",
        "--clearance.heartbeat-evaluator-interval-ms=200",
        "--clearance.heartbeat-evaluator-enabled=true",
        // Tests request a bounded real reconciliation pass via /reconcile.
        // This preserves other fixtures' deliberate unchanged-row assertions.
        "--clearance.reconciliation-enabled=false",
        "--clearance.recovery-mode=" + recoveryMode,
        "--clearance.recovery-generation-log=" + generationLogPath());
  }

  /**
   * Waits until a recovery child JVM persists a generation, polling the authority table
   * over plain JDBC. Throws with a boot-log tail when the child dies or the deadline
   * passes; the caller owns destroying the child.
   */
  private static UUID awaitRecoveryGeneration(String schema, Path bootLog, Process child)
      throws Exception {
    long deadline = System.currentTimeMillis() + 120_000;
    while (System.currentTimeMillis() < deadline) {
      if (!child.isAlive()) {
        throw new IllegalStateException(
            "recovery child JVM exited early; boot log tail: " + logTail(bootLog));
      }
      try (var con = DriverManager.getConnection(jdbcUrl(schema),
              System.getenv("PGUSER"), System.getenv("PGPASSWORD"));
          var ps = con.prepareStatement(
              "SELECT current_generation FROM recovery_authority WHERE singleton = true");
          var rs = ps.executeQuery()) {
        if (rs.next() && rs.getObject(1) != null) {
          return (UUID) rs.getObject(1);
        }
      }
      Thread.sleep(500);
    }
    throw new IllegalStateException(
        "timed out waiting for recovery child generation; boot log tail: " + logTail(bootLog));
  }

  private static String logTail(Path bootLog) {
    try {
      String log = Files.readString(bootLog);
      return log.substring(Math.max(0, log.length() - 2000));
    } catch (Exception ignored) {
      return "<unreadable>";
    }
  }

  public static void main(String[] args) throws Exception {
    Path manifest = Path.of(args[0]);
    String schema = args[1];
    var context = start(schema, 0);
    JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
    AuthProperties auth = context.getBean(AuthProperties.class);
    JobService jobs = context.getBean(JobService.class);
    SchedulerService scheduler = context.getBean(SchedulerService.class);
    String submitToken = "go-submit-" + UUID.randomUUID();
    auth.getApiKeys().put(submitToken, "go-integration");
    UUID spareRunner = UUID.randomUUID();
    jdbc.update("INSERT INTO runners (runner_id, runner_class) VALUES (?, 'default')", spareRunner);
    Map<String, Object> fixtures = new LinkedHashMap<>();
    for (String name : List.of("recovery", "heartbeat", "heartbeat-loss", "reorder", "identity",
        "lifecycle-success", "lifecycle-failure", "lifecycle-cancel", "lifecycle-timeout",
        "lifecycle-kill-fault", "lifecycle-inspect-fault", "lifecycle-scrub-fault",
        "crash-retry", "crash-quarantine", "reconcile-running", "reconcile-finished",
        "reconcile-lost-report", "reconcile-lost-start", "reconcile-stale", "reconcile-orphaned",
        "reconcile-contradictory", "reconcile-insufficient", "reconcile-dirty",
        "reconcile-kill-fault", "reconcile-inspect-fault", "reconcile-reap-fault", "reconcile-scrub-fault",
        "reconcile-restart", "reconcile-terminate", "reconcile-delayed-start")) {
      UUID runner = UUID.randomUUID();
      String token = "go-integration-" + UUID.randomUUID();
      Path marker = manifest.getParent().resolve(name + "-executions");
      boolean lifecycle = name.startsWith("lifecycle-");
      boolean crashRecovery = name.startsWith("crash-");
      boolean reconciliation = name.startsWith("reconcile-");
      Path evidence = manifest.getParent().resolve(name);
      Files.createDirectories(evidence);
      // An external execution counter distinguishes the original process tree
      // from the controller's new attempt. Replaying the old attempt is visible.
      String recoveryWorkload = "printf 'run\\n' >> \"$1/executions\"\n"
          + "if ! mkdir \"$1/first-execution\" 2>/dev/null; then\n"
          + "  printf 'retry workspace' > retry-file\n"
          + "  printf 'retry\\n' >> \"$1/retry-executions\"\n"
          + "  exit 0\nfi\n" + WORKLOAD;
      List<String> argv = crashRecovery
          ? List.of("/bin/sh", "-c", recoveryWorkload, "recovery", evidence.toString(), "0")
          : lifecycle || reconciliation
          ? List.of("/bin/sh", "-c", (reconciliation ? "printf 'run\\n' >> \"$1/executions\"\n" : "") + WORKLOAD, "lifecycle", evidence.toString(),
              name.equals("lifecycle-failure") ? "7" : "0")
          : List.of("/bin/sh", "-c", "printf 'run\\n' >> \"$1\"; exec sleep 60",
              "agent-integration", marker.toString());
      jdbc.update("INSERT INTO runners (runner_id, runner_class) VALUES (?, 'default')", runner);
      auth.getRunnerKeys().put(token, runner);
      UUID job = jobs.submit("go-integration", name, argv, "default").job().jobId();
      // This crosses the transactional proxy: delivery can only observe a committed claim.
      var claim = scheduler.claim(job, runner).orElseThrow();
      if (name.equals("lifecycle-timeout")) {
        // The production claim snapshots its configured timeout. This one fixture uses a
        // shorter immutable allocation deadline, before any agent can observe the claim.
        jdbc.update("UPDATE allocations SET workload_timeout_ms = 5000 WHERE allocation_id = ?",
            claim.allocationId());
      }
      if (name.equals("heartbeat-loss") || reconciliation) {
        jdbc.update("UPDATE allocations SET heartbeat_timeout_ms = ? WHERE allocation_id = ?",
            reconciliation ? 600000 : 2000, claim.allocationId());
      }
      Map<String, Object> fixture = new LinkedHashMap<>();
      fixture.put("runner_id", runner.toString());
      fixture.put("token", token);
      fixture.put("allocation_id", claim.allocationId().toString());
      fixture.put("attempt_id", claim.attemptId().toString());
      fixture.put("job_id", job.toString());
      fixture.put("runner_epoch", claim.runnerEpoch());
      fixture.put("argv", argv);
      fixture.put("marker", marker.toString());
      if (name.equals("heartbeat-loss") || lifecycle || crashRecovery || reconciliation) {
        fixture.put("evidence_dir", evidence.toString());
        List<String> nextArgv = List.of("/bin/sh", "-c", "printf 'next\\n' " + (reconciliation ? ">>" : ">") + " \"$1\"; printf 'scrub me' > next-file",
            "next-allocation", evidence.resolve("next-executed").toString());
        UUID nextJob = jobs.submit("go-integration", name + "-next", nextArgv, "default").job().jobId();
        fixture.put("next_job_id", nextJob.toString());
        fixture.put("next_argv", nextArgv);
        fixture.put("spare_runner_id", spareRunner.toString());
      }
      fixtures.put(name, fixture);
    }
    // Idle-at-backup runners: seeded inventory with no allocation and no claim,
    // so the real Go daemon must advance them via idle RECONCILE (allocation_id
    // omitted). Never-claimed runners keep epoch 0.
    Map<String, Object> idleRunners = new LinkedHashMap<>();
    for (String name : List.of("idle-clean", "idle-dirty")) {
      UUID runner = UUID.randomUUID();
      String token = "go-integration-" + UUID.randomUUID();
      Path evidence = manifest.getParent().resolve(name);
      Files.createDirectories(evidence);
      jdbc.update("INSERT INTO runners (runner_id, runner_class) VALUES (?, 'default')", runner);
      auth.getRunnerKeys().put(token, runner);
      Map<String, Object> idle = new LinkedHashMap<>();
      idle.put("runner_id", runner.toString());
      idle.put("token", token);
      idle.put("evidence_dir", evidence.toString());
      idleRunners.put(name, idle);
    }
    int port = context.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
    // This control server exists only in the standalone test launcher, never in Application.
    // It lets tests invoke the real scheduler and restart the actual Spring controller.
    String controlToken = "harness-" + UUID.randomUUID();
    var control = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    ConfigurableApplicationContext[] current = {context};
    // The control plane lives outside Spring, so the serving controller can be stopped
    // across a rewind (no claims servable while the authority is empty) while control
    // stays up; stashed credentials restore the replacement controller afterwards.
    SavedAuth[] saved = {null};
    control.createContext("/", exchange -> {
      synchronized (current) {
        int status = 200;
        Object response;
        try {
          if (!exchange.getRequestMethod().equals("POST")
              || !controlToken.equals(exchange.getRequestHeaders().getFirst("X-Harness-Token"))) {
            status = 403;
            response = Map.of("error", "forbidden");
          } else if (current[0] == null
              && !exchange.getRequestURI().getPath().equals("/recovery-boot-process")
              && !exchange.getRequestURI().getPath().equals("/restart")
              && !exchange.getRequestURI().getPath().equals("/stop-controller")) {
            status = 503;
            response = Map.of("error", "controller_stopped");
          } else if (exchange.getRequestURI().getPath().equals("/stop-controller")) {
            // Stop the serving controller before a rewind for the PITR drill (issue #40):
            // with the authority about to be empty, a live instance would grant
            // legacy-availability claims on rewound rows. The control plane stays up, so
            // the drill can prove the window serves nothing before rewinding.
            if (current[0] != null) {
              var liveAuth = current[0].getBean(AuthProperties.class);
              saved[0] = new SavedAuth(new LinkedHashMap<>(liveAuth.getRunnerKeys()),
                  new LinkedHashMap<>(liveAuth.getApiKeys()));
              current[0].close();
              current[0] = null;
            }
            response = Map.of("stopped", true);
          } else if (exchange.getRequestURI().getPath().equals("/claim")) {
            var request = JSON.readTree(exchange.getRequestBody().readNBytes(16384));
            UUID runner = UUID.fromString(request.get("runner_id").asString());
            UUID job = UUID.fromString(request.get("job_id").asString());
            var claim = current[0].getBean(SchedulerService.class).claim(job, runner);
            response = claim.<Object>map(c -> Map.of("assigned", true,
                "allocation_id", c.allocationId().toString(), "attempt_id", c.attemptId().toString(),
                "job_id", c.jobId().toString(), "runner_epoch", c.runnerEpoch()))
                .orElseGet(() -> Map.of("assigned", false));
          } else if (exchange.getRequestURI().getPath().equals("/arm")) {
            var request = JSON.readTree(exchange.getRequestBody().readNBytes(16384));
            UUID allocation = UUID.fromString(request.get("allocation_id").asString());
            // Like the deadline fixture above, set the short immutable timeout
            // before the agent can observe the claim, not while it is executing.
            int updated = current[0].getBean(JdbcTemplate.class).update("""
                UPDATE allocations SET heartbeat_timeout_ms = 2000, last_contact_at = now()
                WHERE allocation_id = ? AND agent_incarnation IS NULL AND max_seq = 0
                """, allocation);
            response = Map.of("armed", updated == 1);
          } else if (exchange.getRequestURI().getPath().equals("/reconcile")) {
            var request = JSON.readTree(exchange.getRequestBody().readNBytes(16384));
            UUID allocation = UUID.fromString(request.get("allocation_id").asString());
            response = Map.of("requested", current[0].getBean(ReconciliationService.class)
                .evaluateAllocation(allocation));
          } else if (exchange.getRequestURI().getPath().equals("/enter-recovery")) {
            UUID generation = current[0].getBean(RecoveryService.class).enterRecoveryMode();
            response = Map.of("generation", generation.toString());
          } else if (exchange.getRequestURI().getPath().equals("/recovery-boot-process")) {
            // New-OS/JVM-process recovery boot for the rewind drill (issue #40): stops the
            // serving controller if live, boots a child JVM running the plain controller
            // with clearance.recovery-mode=true against the restored schema, waits until
            // its RecoveryBootRunner persists a fresh generation, asserts freshness and
            // quarantine from the retained child boot log, destroys the child, then starts
            // a normal-mode serving context (preserving the fresh authority) with the
            // stashed credentials. The child exercises a genuinely fresh JVM (own args/env,
            // static state, FDs, Flyway-plus-runners startup); physical PITR replay itself
            // remains #40 work.
            if (current[0] != null) {
              var liveAuth = current[0].getBean(AuthProperties.class);
              saved[0] = new SavedAuth(new LinkedHashMap<>(liveAuth.getRunnerKeys()),
                  new LinkedHashMap<>(liveAuth.getApiKeys()));
              current[0].close();
              current[0] = null;
            }
            if (saved[0] == null) {
              throw new IllegalStateException("no stashed credentials for recovery boot");
            }
            List<String> command = new java.util.ArrayList<>(List.of(
                System.getProperty("java.home") + "/bin/java",
                "-cp", System.getProperty("java.class.path"),
                "com.clearance.controller.Application"));
            command.addAll(controllerArgs(schema, 0, true));
            Path bootLog = manifest.getParent().resolve(
                "recovery-boot-child-" + System.currentTimeMillis() + ".log");
            Process child = new ProcessBuilder(command)
                .redirectOutput(bootLog.toFile())
                .redirectErrorStream(true)
                .start();
            try {
              UUID childGeneration = awaitRecoveryGeneration(schema, bootLog, child);
              String childBootLog = Files.readString(bootLog);
              if (!childBootLog.contains("Recovery-mode boot requested")
                  || !childBootLog.contains(childGeneration.toString())) {
                throw new IllegalStateException(
                    "child boot log missing RecoveryBootRunner evidence: " + bootLog);
              }
              current[0] = start(schema, port, false);
              var restoredAuth = current[0].getBean(AuthProperties.class);
              restoredAuth.setRunnerKeys(saved[0].runnerKeys());
              restoredAuth.setApiKeys(saved[0].apiKeys());
              response = Map.of("restarted", true, "os_process", true,
                  "generation", childGeneration.toString(),
                  "boot", "RecoveryBootRunner with clearance.recovery-mode=true "
                      + "in a new OS/JVM child process",
                  "boot_log", bootLog.toAbsolutePath().toString());
            } finally {
              child.destroyForcibly();
              try {
                child.waitFor(30, TimeUnit.SECONDS);
              } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
              }
            }
          } else if (exchange.getRequestURI().getPath().equals("/submit")) {
            var request = JSON.readTree(exchange.getRequestBody().readNBytes(16384));
            String name = request.has("name") ? request.get("name").asString() : "idle-next";
            List<String> argv = List.of("echo", "hi");
            if (request.has("argv") && request.get("argv").isArray() && !request.get("argv").isEmpty()) {
              argv = new java.util.ArrayList<>();
              for (var element : request.get("argv")) {
                argv.add(element.asString());
              }
            }
            UUID job = current[0].getBean(JobService.class)
                .submit("go-integration", name, argv, "default").job().jobId();
            response = Map.of("job_id", job.toString());
          } else if (exchange.getRequestURI().getPath().equals("/restart")) {
            if (current[0] != null) {
              var priorAuth = current[0].getBean(AuthProperties.class);
              saved[0] = new SavedAuth(new LinkedHashMap<>(priorAuth.getRunnerKeys()),
                  new LinkedHashMap<>(priorAuth.getApiKeys()));
              current[0].close();
            }
            if (saved[0] == null) {
              throw new IllegalStateException("no stashed credentials for restart");
            }
            current[0] = start(schema, port);
            var restoredAuth = current[0].getBean(AuthProperties.class);
            restoredAuth.setRunnerKeys(saved[0].runnerKeys());
            restoredAuth.setApiKeys(saved[0].apiKeys());
            response = Map.of("restarted", true);
          } else {
            status = 404;
            response = Map.of("error", "not_found");
          }
        } catch (Exception failure) {
          failure.printStackTrace();
          status = 500;
          response = Map.of("error", failure.toString());
        }
        byte[] body = JSON.writeValueAsString(response).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
      }
    });
    control.start();
    Files.writeString(manifest, JSON.writeValueAsString(Map.of(
        "url", "http://127.0.0.1:" + port, "schema", schema, "fixtures", fixtures,
        "idle_runners", idleRunners,
        "submit_token", submitToken, "control_url", "http://127.0.0.1:" + control.getAddress().getPort(),
        "control_token", controlToken, "spare_runner_id", spareRunner.toString())));
  }
}
