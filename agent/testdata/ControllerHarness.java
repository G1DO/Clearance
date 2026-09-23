import com.clearance.controller.Application;
import com.clearance.controller.auth.AuthProperties;
import com.clearance.controller.jobs.JobService;
import com.clearance.controller.scheduling.SchedulerService;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.ObjectMapper;

/** Test-only launcher: the Go suite drives the actual controller and scheduler on PostgreSQL. */
public class ControllerHarness {
  private static final ObjectMapper JSON = new ObjectMapper();
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
    String jdbcUrl = "jdbc:postgresql://" + System.getenv("PGHOST") + ":"
        + System.getenv("PGPORT") + "/" + System.getenv("PGDATABASE")
        + "?currentSchema=" + schema;
    return new SpringApplicationBuilder(Application.class).web(WebApplicationType.SERVLET)
        .run("--server.port=" + port, "--spring.profiles.active=test",
            "--spring.datasource.url=" + jdbcUrl,
            "--spring.datasource.username=" + System.getenv("PGUSER"),
            "--spring.datasource.password=" + System.getenv("PGPASSWORD"),
            "--spring.flyway.schemas=" + schema,
            "--spring.flyway.default-schema=" + schema);
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
    Map<String, Object> fixtures = new LinkedHashMap<>();
    for (String name : List.of("recovery", "heartbeat", "reorder", "identity",
        "lifecycle-success", "lifecycle-failure", "lifecycle-cancel", "lifecycle-timeout",
        "lifecycle-kill-fault", "lifecycle-inspect-fault", "lifecycle-scrub-fault")) {
      UUID runner = UUID.randomUUID();
      String token = "go-integration-" + UUID.randomUUID();
      Path marker = manifest.getParent().resolve(name + "-executions");
      boolean lifecycle = name.startsWith("lifecycle-");
      Path evidence = manifest.getParent().resolve(name);
      Files.createDirectories(evidence);
      List<String> argv = lifecycle
          ? List.of("/bin/sh", "-c", WORKLOAD, "lifecycle", evidence.toString(),
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
      Map<String, Object> fixture = new LinkedHashMap<>();
      fixture.put("runner_id", runner.toString());
      fixture.put("token", token);
      fixture.put("allocation_id", claim.allocationId().toString());
      fixture.put("attempt_id", claim.attemptId().toString());
      fixture.put("job_id", job.toString());
      fixture.put("runner_epoch", claim.runnerEpoch());
      fixture.put("argv", argv);
      fixture.put("marker", marker.toString());
      if (lifecycle) {
        fixture.put("evidence_dir", evidence.toString());
        List<String> nextArgv = List.of("/bin/sh", "-c", "printf 'next\\n' > \"$1\"; printf 'scrub me' > next-file",
            "next-allocation", evidence.resolve("next-executed").toString());
        UUID nextJob = jobs.submit("go-integration", name + "-next", nextArgv, "default").job().jobId();
        fixture.put("next_job_id", nextJob.toString());
        fixture.put("next_argv", nextArgv);
      }
      fixtures.put(name, fixture);
    }
    int port = context.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
    // This control server exists only in the standalone test launcher, never in Application.
    // It lets tests invoke the real scheduler and restart the actual Spring controller.
    String controlToken = "harness-" + UUID.randomUUID();
    var control = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    ConfigurableApplicationContext[] current = {context};
    control.createContext("/", exchange -> {
      synchronized (current) {
        int status = 200;
        Object response;
        try {
          if (!exchange.getRequestMethod().equals("POST")
              || !controlToken.equals(exchange.getRequestHeaders().getFirst("X-Harness-Token"))) {
            status = 403;
            response = Map.of("error", "forbidden");
          } else if (exchange.getRequestURI().getPath().equals("/claim")) {
            var request = JSON.readTree(exchange.getRequestBody().readNBytes(16384));
            UUID runner = UUID.fromString(request.get("runner_id").asString());
            UUID job = UUID.fromString(request.get("job_id").asString());
            var claim = current[0].getBean(SchedulerService.class).claim(job, runner);
            response = claim.<Object>map(c -> Map.of("assigned", true,
                "allocation_id", c.allocationId().toString(), "attempt_id", c.attemptId().toString(),
                "job_id", c.jobId().toString(), "runner_epoch", c.runnerEpoch()))
                .orElseGet(() -> Map.of("assigned", false));
          } else if (exchange.getRequestURI().getPath().equals("/restart")) {
            var priorAuth = current[0].getBean(AuthProperties.class);
            var runnerKeys = new LinkedHashMap<>(priorAuth.getRunnerKeys());
            var apiKeys = new LinkedHashMap<>(priorAuth.getApiKeys());
            current[0].close();
            current[0] = start(schema, port);
            var restoredAuth = current[0].getBean(AuthProperties.class);
            restoredAuth.setRunnerKeys(runnerKeys);
            restoredAuth.setApiKeys(apiKeys);
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
        "submit_token", submitToken, "control_url", "http://127.0.0.1:" + control.getAddress().getPort(),
        "control_token", controlToken)));
  }
}
