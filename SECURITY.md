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
- Keys in [application.yml](controller/src/main/resources/application.yml) are dev-only
  test keys; the bundled configuration is for local development.
- Never commit production keys. Supply configuration explicitly through the process
  environment or an external Spring configuration file. `.env` is ignored by Git,
  but neither the controller startup command nor the agent automatically loads it;
  export the required values into the process environment.
- Cross-project GET returns 404 with no existence oracle. This is intentional.
- See [job intake](docs/design/specifications/job-intake.md) for project isolation,
  [agent API](docs/design/specifications/agent-api.md) for machine authentication and
  report fencing, and [agent operation](agent/README.md) for durable-state protection
  and trusted-workload limitations.
