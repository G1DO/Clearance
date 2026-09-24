# Standalone runner agent

`cmd/clearance-agent` is the Linux Go daemon. It uses the Go standard library and
requires Go 1.23+. From this directory:

```sh
go build -o target/clearance-agent ./cmd/clearance-agent
./target/clearance-agent -help
```

- [Operation and restart safety](../docs/operations/agent.md): startup prerequisites,
  containment, durable state, cleanup, and failure triage.
- [Verification and diagnostics](../docs/development/agent-verification.md): delegated
  Linux checks, controller integration, SIGKILL recovery drill, and hygiene soak.
- [Agent API](../docs/api/agent-api.md) and [wire contract](../contracts/agent-v1/README.md):
  authentication, allocation delivery, report fencing, and compatibility fixtures.
- [Security boundaries](../docs/security/README.md): host and credential trust.
