# Configuration reference

Lookup for environment variables, flags, and controller settings.
Behavior lives in the linked guides and code; this file does not restate it.

| Name | Default | Source |
| --- | --- | --- |
| `CLEARANCE_MACHINE_TOKEN` | — (required) | [agent operation](../operations/agent.md), `agent/cmd/clearance-agent/main.go` |
| `-controller`, `-state-dir`, `-cgroup-root`, `-workspace-root` | — (required) | [agent operation](../operations/agent.md), `agent/cmd/clearance-agent/main.go` |
| `SPRING_CONFIG_LOCATION` | bundled `application.yml` | [security configuration](../security/README.md#controller-configuration) |
| `clearance.workload-timeout-ms` | `3600000` (`1..86400000`) | [application.yml](../../controller/src/main/resources/application.yml), [agent API](../api/agent-api.md) |
| `clearance.heartbeat-timeout-ms` | `15000` (`1..86400000`) | [application.yml](../../controller/src/main/resources/application.yml), [agent API](../api/agent-api.md#heartbeat-timeout-and-quarantine) |
| `clearance.heartbeat-evaluator-interval-ms` / `-batch-size` / `-enabled` | `1000` / `50` / `true` | [application.yml](../../controller/src/main/resources/application.yml), [agent API](../api/agent-api.md#heartbeat-timeout-and-quarantine) |
| `clearance.reconciliation-interval-ms` / `-batch-size` / `-enabled` | `1000` / `50` / `true` | [application.yml](../../controller/src/main/resources/application.yml), [agent API](../api/agent-api.md#quarantine-reconciliation) |
| `clearance.recovery-mode` | `false` | [application.yml](../../controller/src/main/resources/application.yml), [controller operation](../operations/controller.md) |
| `clearance.recovery-generation` | `""` (auto-issue UUIDv7) | [application.yml](../../controller/src/main/resources/application.yml), [agent API](../api/agent-api.md#recovery-generation-authority) |
| `clearance.recovery-generation-log` | `/var/lib/clearance/recovery-generations.log` | [application.yml](../../controller/src/main/resources/application.yml), [controller operation](../operations/controller.md) |
| `clearance.auth.api-keys` / `runner-keys` | dev keys / `{}` | [application.yml](../../controller/src/main/resources/application.yml), [security configuration](../security/README.md) |
| `server.port`, datasource `localhost:5544`, `clearance`/`clearance` | `8080` | [application.yml](../../controller/src/main/resources/application.yml), [docker-compose.yml](../../docker-compose.yml) |
| `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE` | `localhost`, `5544`, `clearance`, `clearance`, `clearance` | [agent verification](../development/agent-verification.md), `.github/workflows/ci.yml` |
| `AGENT_CGROUP_TEST_ROOT`, `CLEARANCE_CGROUP_ROOT` | — (Linux verification only) | [agent verification](../development/agent-verification.md#isolated-delegated-linux-verification-target) |
| `AGENT_SOAK`, `AGENT_SOAK_DURATION` | unset (`1` / `5s` for soak/smoke) | [agent verification](../development/agent-verification.md#hygiene-soak-and-profiles) |
| `WIRE_EXPORT_DIR`, `WIRE_PEER_DIR` | set by `verify.sh` | [wire contract](../../contracts/agent-v1/README.md#compatibility-fixtures-and-matrix) |

HikariCP `maximum-pool-size: 10` and Tomcat `threads.max: 50` are finite bounds
reused unchanged; see [application.yml](../../controller/src/main/resources/application.yml).
