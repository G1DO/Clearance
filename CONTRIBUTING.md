# Contributing to Clearance

Short-lived branch to implement plus verify to local checks plus self-review to pull request to CI plus review to merge to main.
Keep each PR one coherent reviewable change. Update affected docs in the same PR and link to canonical truth instead of duplicating it.

## Toolchain

- Python 3.12 plus for clearance reference model and tests
- Java 25 and Spring Boot 4.1.1 for controller (Temurin 25 works locally)
- Go 1.23 plus for thin agent codec (`agent/`)
- Docker for PostgreSQL 17: docker compose up -d postgres uses host port 5544, db and user clearance, volume clearance-pgdata
- Controller starts against an empty database via Flyway V1-V4 (V3 adds runners/attempts/allocations; V4 adds agent incarnation, sequence, and report status) and restart revalidates without drift

## Verify

- Run: python -m unittest discover -s tests -v
- Run: cd agent and go test ./... -v (shared agent-v1 fixture matrix, also in CI)
- With PostgreSQL up, run: cd controller and ./mvnw test (CI uses ./mvnw -B -ntp test)
- Complete DB-free Java↔Go exchange: bash contracts/agent-v1/verify.sh (logs and JSON under controller/target/agent-wire-contract/; CI uploads agent-v1-compatibility)
- Standalone Java contract check: cd controller and ./mvnw -B -ntp -Dtest=com.clearance.controller.AgentWireContractTest test
- To run locally: docker compose up -d postgres, then cd controller and ./mvnw spring-boot:run (serves :8080, uses localhost:5544 per controller/src/main/resources/application.yml)
- Java integration tests need PostgreSQL on localhost 5544 and cover HTTP plus canonicalization plus concurrency plus retry plus restart plus auth plus migrations plus exclusive runner-claim contention and fenced agent polling/reporting
- See docs/design/specifications/job-intake.md, docs/design/specifications/runner-ownership-semantics.md, docs/design/specifications/runner-claim.md, docs/design/specifications/agent-api.md, and contracts/agent-v1/README.md for what the checks prove and their bounds

## Auth and configuration

- clearance.auth.api-keys in controller/src/main/resources/application.yml holds dev-only test keys and is overridable via environment
- Never commit production keys. .env is ignored and can carry local overrides
- project_id always comes from the authenticated key scope, never from the request body
- clearance.auth.runner-keys maps separate machine tokens to UUIDs of directly seeded runners; it defaults to an empty map. Runner keys authorize agent routes only, and submit keys cannot call internal routes
- Agent transport fault controls exist only with the test profile; see docs/design/specifications/agent-api.md. Do not enable the test profile in deployments

## Pull requests

- Explain what changed, why, and how it was verified, plus checks intentionally not run and the issue it closes
- CI must be green. Do not weaken tests, validation, security controls, or required checks to force green
