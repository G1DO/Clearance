# Security

## Reporting

Do not put undisclosed vulnerability details in a public Issue.
Contact the maintainer privately and give time to triage before any disclosure.
Use a private channel such as a GitHub Security Advisory when available.

## v1 scope

- API auth is a static API-key to project mapping for v1 only.
- Keys in controller/src/main/resources/application.yml are dev-only test keys.
- Never commit production keys. Override via environment. .env is ignored.
- Cross-project GET returns 404 with no existence oracle. This is intentional.
- See docs/design/specifications/job-intake.md for the auth contract and project isolation behavior.
