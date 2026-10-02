-- V9 recovery generation authority (issue #32).
--
-- PostgreSQL remains the durable ownership authority; this table persists the
-- single current recovery generation as fleet authority. Generation values are
-- random UUIDv4 issued on recovery-mode boot, never derived from rewound
-- database state, so a restore that rewinds this table cannot cause a repeat:
-- the next recovery boot issues a fresh random value that has never been used.
--
-- runners.reconciled_generation is the last generation a runner reconciled
-- under (advanced by per-runner reconciliation in a later issue; NULL means
-- never reconciled under any generation). allocations.recovery_generation is
-- the generation current at claim time, used for stale-generation fencing.
-- Recovery boot quarantines the whole fleet; claims require
-- reconciled_generation to equal the current authority whenever an authority
-- row exists. Reports on allocations whose generation differs from current
-- are rejected with zero mutation.
CREATE TABLE IF NOT EXISTS recovery_authority (
    singleton BOOLEAN PRIMARY KEY CHECK (singleton),
    current_generation UUID NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE runners ADD COLUMN reconciled_generation UUID;

ALTER TABLE allocations ADD COLUMN recovery_generation UUID;

CREATE INDEX IF NOT EXISTS ix_runners_reconciled_generation
    ON runners (reconciled_generation);

CREATE INDEX IF NOT EXISTS ix_allocations_recovery_generation
    ON allocations (recovery_generation);
