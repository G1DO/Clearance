-- V3 exclusive runner claim under PostgreSQL authority (issue #5).
--
-- Durable identities (distinct roles, never collapsed):
--   jobs.job_id        logical job identity (V2).
--   attempts.attempt_id execution-attempt identity, one row per claim.
--   allocations.allocation_id ownership identity binding one attempt to one runner.
--   runners.epoch      runner ownership generation, advanced monotonically per claim.
--
-- Correctness strategy (READ COMMITTED, explicit in SchedulerService):
--   The claim path performs a single conditional UPDATE on runners
--     (state = 'AVAILABLE' AND runner_class = <job class>)
--   which takes the row lock; a concurrent claimant blocks on that row until the
--   winner commits or rolls back, then re-evaluates the predicate and finds no
--   row when the winner committed. A partial UNIQUE index independently rejects
--   a second ACTIVE allocation for the same runner, so a scheduler-selection
--   mistake cannot silently double-allocate.
CREATE TABLE IF NOT EXISTS runners (
    runner_id UUID PRIMARY KEY,
    runner_class TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'AVAILABLE',
    epoch BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_runners_runner_class CHECK (runner_class <> '' AND char_length(runner_class) <= 128),
    CONSTRAINT chk_runners_state CHECK (state IN ('AVAILABLE', 'ASSIGNED', 'STARTING', 'RUNNING', 'CLEANING', 'QUARANTINED')),
    CONSTRAINT chk_runners_epoch CHECK (epoch >= 0)
);

CREATE TABLE IF NOT EXISTS attempts (
    attempt_id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs (job_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS allocations (
    allocation_id UUID PRIMARY KEY,
    attempt_id UUID NOT NULL UNIQUE REFERENCES attempts (attempt_id),
    job_id UUID NOT NULL REFERENCES jobs (job_id),
    runner_id UUID NOT NULL REFERENCES runners (runner_id),
    runner_epoch BIGINT NOT NULL,
    state TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_allocations_state CHECK (state = 'ACTIVE'),
    CONSTRAINT chk_allocations_runner_epoch CHECK (runner_epoch > 0)
);

-- One authoritative active allocation per runner, enforced by PostgreSQL itself.
CREATE UNIQUE INDEX IF NOT EXISTS uq_allocations_runner_active
    ON allocations (runner_id) WHERE state = 'ACTIVE';

CREATE INDEX IF NOT EXISTS ix_runners_class_state ON runners (runner_class, state);
CREATE INDEX IF NOT EXISTS ix_attempts_job ON attempts (job_id);
CREATE INDEX IF NOT EXISTS ix_allocations_job ON allocations (job_id);
