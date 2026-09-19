# Contributing to Clearance

Short-lived branch to implement plus verify to local checks plus self-review to pull request to CI plus review to merge to main.
Keep each PR one coherent reviewable change. Update affected docs in the same PR and link to canonical truth instead of duplicating it.

## Toolchain

- Python 3.12 plus for clearance reference model and tests
- Java 25 and Spring Boot 4.1.1 for controller (Temurin 25 works locally)
- Docker for PostgreSQL 17: docker compose up -d postgres uses host port 5544, db and user clearance, volume clearance-pgdata
- Controller starts against an empty database via Flyway V1 plus V2 and restart revalidates without drift

## Verify

- Run: python -m unittest discover -s tests -v
- With PostgreSQL up, run: cd controller and ./mvnw test
- Java integration tests need PostgreSQL on localhost 5544 and cover HTTP plus canonicalization plus concurrency plus retry plus restart plus auth plus migrations
- See docs/design/specifications/job-intake.md and docs/design/specifications/runner-ownership-semantics.md for what the checks prove and their bounds

## Auth and configuration

- clearance.auth.api-keys in controller/src/main/resources/application.yml holds dev-only test keys and is overridable via environment
- Never commit production keys. .env is ignored and can carry local overrides
- project_id always comes from the authenticated key scope, never from the request body

## Pull requests

- Explain what changed, why, and how it was verified, plus checks intentionally not run and the issue it closes
- CI must be green. Do not weaken tests, validation, security controls, or required checks to force green
