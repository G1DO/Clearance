import com.clearance.controller.Application;
import com.clearance.controller.auth.AuthProperties;
import com.clearance.controller.jobs.JobService;
import com.clearance.controller.scheduling.SchedulerService;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.ObjectMapper;

/** Test-only launcher: the Go suite drives the actual controller and scheduler on PostgreSQL. */
public class ControllerHarness {
  public static void main(String[] args) throws Exception {
    Path manifest = Path.of(args[0]);
    String schema = args[1];
    String jdbcUrl = "jdbc:postgresql://" + System.getenv("PGHOST") + ":"
        + System.getenv("PGPORT") + "/" + System.getenv("PGDATABASE")
        + "?currentSchema=" + schema;
    var context = new SpringApplicationBuilder(Application.class).web(WebApplicationType.SERVLET)
        .run("--server.port=0", "--spring.profiles.active=test",
            "--spring.datasource.url=" + jdbcUrl,
            "--spring.datasource.username=" + System.getenv("PGUSER"),
            "--spring.datasource.password=" + System.getenv("PGPASSWORD"),
            "--spring.flyway.schemas=" + schema,
            "--spring.flyway.default-schema=" + schema);
    JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
    AuthProperties auth = context.getBean(AuthProperties.class);
    JobService jobs = context.getBean(JobService.class);
    SchedulerService scheduler = context.getBean(SchedulerService.class);
    Map<String, Object> fixtures = new LinkedHashMap<>();
    for (String name : List.of("recovery", "heartbeat", "reorder", "identity")) {
      UUID runner = UUID.randomUUID();
      String token = "go-integration-" + UUID.randomUUID();
      Path marker = manifest.getParent().resolve(name + "-executions");
      // A direct process is left running so heartbeat and cancellation paths are exercised.
      // Positional arguments preserve paths as data rather than shell interpolation.
      List<String> argv = List.of("/bin/sh", "-c", "printf 'run\\n' >> \"$1\"; exec sleep 60",
          "agent-integration", marker.toString());
      jdbc.update("INSERT INTO runners (runner_id, runner_class) VALUES (?, 'default')", runner);
      auth.getRunnerKeys().put(token, runner);
      UUID job = jobs.submit("go-integration", name, argv, "default").job().jobId();
      // This crosses the transactional proxy: delivery can only observe a committed claim.
      var claim = scheduler.claim(job, runner).orElseThrow();
      Map<String, Object> fixture = new LinkedHashMap<>();
      fixture.put("runner_id", runner.toString());
      fixture.put("token", token);
      fixture.put("allocation_id", claim.allocationId().toString());
      fixture.put("job_id", job.toString());
      fixture.put("runner_epoch", claim.runnerEpoch());
      fixture.put("argv", argv);
      fixture.put("marker", marker.toString());
      fixtures.put(name, fixture);
    }
    int port = context.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
    Files.writeString(manifest, new ObjectMapper().writeValueAsString(Map.of(
        "url", "http://127.0.0.1:" + port, "schema", schema, "fixtures", fixtures)));
  }
}
