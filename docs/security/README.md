# Security boundaries and configuration

## Authentication and authorization

- Job API auth is a static API-key to project mapping for v1 only.
- Internal agent auth uses a separate `clearance.auth.runner-keys` token-to-runner UUID mapping
  for seeded inventory. Machine credentials cannot authorize `/api/**`, and submit credentials
  cannot authorize `/internal/**`. Tokens configured in both maps are rejected by both APIs.
- A body `runner_id` is only an optional assertion; authenticated machine identity determines
  authority. Cross-runner reports and operator-only actions are rejected before writes.
- Fault controls are registered only under the `test` profile and return 404 outside it.
  Never enable `test` in deployments.
- Cross-project GET returns 404 with no existence oracle. This is intentional.
- See [job intake](../api/job-intake.md) for project isolation,
  [agent API](../api/agent-api.md) for machine authentication and
  report fencing, and [agent operation](../operations/agent.md) for durable-state protection
  and trusted-workload limitations.

## Controller configuration

The bundled [application.yml](../../controller/src/main/resources/application.yml) is for
local development and includes development API keys. Spring merges credential-map
entries across configuration sources: adding new keys through the environment or
an additional configuration file leaves the bundled keys active.

Before starting a controller outside local development, set
`SPRING_CONFIG_LOCATION=file:/absolute/path/clearance.yml` to replace the default
configuration locations with a required external file. Supply the intended API-key
and runner-key mappings, database connection, and runtime settings there; retain the
finite connection-pool and request-thread bounds from the bundled configuration.
Keep credentials out of version control and restrict access to the file. See
[Spring Boot external configuration](https://docs.spring.io/spring-boot/reference/features/external-config.html)
for location replacement and map binding rules.

Verify the effective authentication configuration before exposing the controller:
send `GET /api/v1/jobs/{jobId}` with a valid but nonexistent UUID. Both bundled
development keys must return 401; an intended submit key must return 404.
Keep the `test` profile disabled.

`.env` is ignored by Git, but neither the controller startup command nor the agent
automatically loads it; export required values into the process environment.

## Workload and transport trust

Only trusted job authors and workloads may use these runners. The
[Linux launch path](../../agent/containment_linux.go) changes the working directory
and cgroup membership; it does not change the agent's OS identity, filter its
environment, or create filesystem/network namespaces. Commands inherit the agent
environment, including `CLEARANCE_MACHINE_TOKEN`, and can access host resources
available to that user. Project-scoped job access is not host isolation between
projects. Cgroups provide execution tracking and cleanup, not a hostile-workload
sandbox. Workloads must not alter the agent's state, namespaces, mounts, or cgroups.

Protect the state directory, containment journal, controller configuration, and
diagnostic copies against unauthorized access. State and database records can
contain command arguments and host paths; avoid putting secrets in `argv`.

The [agent HTTP client](../../agent/client.go) sends the machine bearer token on
every request, accepts HTTP or HTTPS origins, honors environment proxy settings,
and rejects redirects. The bundled controller configuration does not enable TLS.
Use HTTPS with a trusted TLS endpoint for traffic across hosts; protect any
proxy-to-controller hop and configure proxy environment variables deliberately.
Local HTTP examples do not establish transport protection for a deployment.

Report suspected vulnerabilities through [the private reporting policy](../../SECURITY.md).
