# Security

## Reporting

Do not put undisclosed vulnerability details in a public Issue.
Contact the maintainer privately and give time to triage before any disclosure.
Use a private channel such as a GitHub Security Advisory when available.

## v1 scope

- Job API auth is a static API-key to project mapping for v1 only.
- Internal agent auth uses a separate `clearance.auth.runner-keys` token-to-runner UUID mapping
  for seeded inventory. Machine credentials cannot authorize `/api/**`, and submit credentials
  cannot authorize `/internal/**`. Tokens configured in both maps are rejected by both APIs.
- A body `runner_id` is only an optional assertion; authenticated machine identity determines
  authority. Cross-runner reports and operator-only actions are rejected before writes.
- Fault controls are registered only under the `test` profile and return 404 outside it.
  Never enable `test` in deployments.
- Keys in controller/src/main/resources/application.yml are dev-only test keys.
- Never commit production keys. Override via environment. .env is ignored.
- Cross-project GET returns 404 with no existence oracle. This is intentional.
- See docs/design/specifications/job-intake.md for project isolation and
  docs/design/specifications/agent-api.md for machine authentication and report fencing.
