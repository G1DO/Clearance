-- V2 project-scoped idempotent jobs (issue #4).
-- Single durable table holds both identities:
--   operation_id (client mutation identity, scoped by project_id) with UNIQUE(project_id, operation_id)
--   job_id (logical job identity, primary key).
-- Idempotency converges via INSERT ... ON CONFLICT (project_id, operation_id) DO NOTHING;
-- PostgreSQL is the sole authority (no in-memory dedup, no external coordination).
-- Canonical semantic request is (argv, runnerClass); payload_hash is SHA-256 hex of
-- the canonical JSON defined in docs/design/specifications/job-intake.md.
CREATE TABLE IF NOT EXISTS jobs (
    job_id UUID PRIMARY KEY,
    project_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    argv JSONB NOT NULL,
    runner_class TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_jobs_project_operation UNIQUE (project_id, operation_id),
    CONSTRAINT chk_jobs_project_nonempty CHECK (project_id <> ''),
    CONSTRAINT chk_jobs_operation_nonempty CHECK (operation_id <> '' AND char_length(operation_id) <= 128),
    CONSTRAINT chk_jobs_runner_class CHECK (runner_class <> '' AND char_length(runner_class) <= 128),
    CONSTRAINT chk_jobs_payload_hash CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT chk_jobs_argv_nonempty_array CHECK (jsonb_typeof(argv) = 'array' AND jsonb_array_length(argv) > 0)
);

CREATE INDEX IF NOT EXISTS ix_jobs_project ON jobs (project_id);
