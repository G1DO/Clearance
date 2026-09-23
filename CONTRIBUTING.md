# Contributing to Clearance

The canonical [Workflow](https://app.notion.com/p/Workflow-3bb0a821b3cc817394cdf93a936a3612) defines engineering flow; [Documentation](https://app.notion.com/p/Documentation-3bb0a821b3cc815099acfd2d5e8b0859) defines documentation responsibilities. This guide covers working in this repository.

## Toolchain and local setup

- Python 3.12+ for the reference model and tests; no third-party Python dependencies.
- Java 25 for the controller. The [Maven build](controller/pom.xml) pins Spring Boot and dependencies; use the checked-in Maven wrapper.
- Go 1.23+ on Linux for the agent; [go.mod](agent/go.mod) declares the language version.
- Docker Compose for PostgreSQL 17 and `psql` for the agent integration harness.

From the repository root:

```sh
docker compose up -d --wait postgres
cd controller
./mvnw spring-boot:run
```

Compose waits for PostgreSQL to be healthy before the controller starts. The controller serves port 8080. [docker-compose.yml](docker-compose.yml) defines the local database and persistent volume; [application.yml](controller/src/main/resources/application.yml) contains the matching controller defaults (`localhost:5544`, database/user/password `clearance`). Flyway applies the [versioned migrations](controller/src/main/resources/db/migration/) on startup and validates them on restart. Add a new migration when changing an applied schema; preserve existing migration history.

Use a dedicated development/test database: integration tests write jobs, runners, and allocations. Keep the controller suite and agent integration harness sequential because some schema assertions count indexes across the database.

The controller does not schedule submitted jobs automatically. See [current implementation limits](README.md#current-implementation) and [runner seeding and claims](docs/design/specifications/runner-claim.md). Agent startup and state-directory requirements are in the [agent guide](agent/README.md).

## Verification

Run from the repository root unless a command changes directory:

| Check | Command | Prerequisites |
| --- | --- | --- |
| Ownership model and exploration | `python3 -m unittest discover -s tests -v` | Python; CI uses its configured `python` executable. |
| Agent vet and race checks | `(cd agent && go vet ./... && go test -race ./...)` | Go on Linux with a C compiler for the race detector. |
| Controller suite | `(cd controller && ./mvnw -B -ntp test)` | Java and the local PostgreSQL database. |
| Java↔Go wire exchange | `bash contracts/agent-v1/verify.sh` | Java and Go; database-free. |

Also run the [real-controller agent integration](agent/README.md#verification-and-bounded-diagnostics) and the defined five-agent, five-minute hygiene soak when verifying the full agent runtime. That guide owns their commands, prerequisites, evidence paths, and bounds. The [wire contract guide](contracts/agent-v1/README.md#compatibility-fixtures-and-matrix) owns standalone codec commands and exchange evidence.

[CI](.github/workflows/ci.yml) runs `python`, `agent-contract`, `agent-runtime`, and `java` on PRs and pushes to `main`. These include the full soak and real-controller integration. CI configuration is the source of truth for tool versions, checks, and artifact retention; test output belongs in CI/artifacts rather than copied into specifications.

There is no repository documentation linter or renderer. For documentation changes, check relative links and section anchors, inspect Markdown rendering, and validate changed commands against their source scripts/configuration.

## Auth and configuration

See [SECURITY.md](SECURITY.md) for credential boundaries, development keys, and environment handling. Configuration defaults live in [application.yml](controller/src/main/resources/application.yml). Agent machine-token and durable-state setup live in the [agent guide](agent/README.md).

## Pull requests

- Explain what changed, why, and what was actually verified. State relevant checks intentionally not run; CI owns routine machine results.
- Link an Issue when one exists. Use `Closes #...` only when merge satisfies that Issue's definition of Done; delivery or target verification may require keeping it open.
- Update affected technical docs in the same PR and link canonical context/evidence instead of copying it.
- Review the final diff and record relevant self-review findings. Satisfy repository review/protection requirements and all required checks before merging. Do not weaken tests, validation, security controls, or required checks to force green.
