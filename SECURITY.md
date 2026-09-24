# Security

## Reporting

Do not put undisclosed vulnerability details in a public Issue.
Use [GitHub private vulnerability reporting](https://github.com/G1DO/Clearance/security/advisories/new)
to contact the maintainer, and give time to triage before any disclosure.

## v1 scope

- Job API auth is a static API-key to project mapping for v1 only.
- Internal agent auth uses a separate `clearance.auth.runner-keys` token-to-runner UUID mapping
  for seeded inventory. Machine credentials cannot authorize `/api/**`, and submit credentials
  cannot authorize `/internal/**`. Tokens configured in both maps are rejected by both APIs.
- A body `runner_id` is only an optional assertion; authenticated machine identity determines
  authority. Cross-runner reports and operator-only actions are rejected before writes.
- Fault controls are registered only under the `test` profile and return 404 outside it.
  Never enable `test` in deployments.
- Cross-project GET returns 404 with no existence oracle. This is intentional.
- See [job intake](docs/design/specifications/job-intake.md) for project isolation,
  [agent API](docs/design/specifications/agent-api.md) for machine authentication and
  report fencing, and [agent operation](agent/README.md) for durable-state protection
  and trusted-workload limitations.

## Controller configuration

The bundled [application.yml](controller/src/main/resources/application.yml) is for
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
