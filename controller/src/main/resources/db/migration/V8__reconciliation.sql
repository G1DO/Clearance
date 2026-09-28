-- V8 physical reconciliation for quarantined runners (issue #27).
-- Quarantine exits only through an explicit, classified reconciliation resolution
-- with fresh current evidence. Ordinary heartbeats, terminal reports, cached
-- observations, or unsolicited cleanup proof cannot clear quarantine.
ALTER TABLE allocations
    ADD COLUMN reconcile_requested BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN reconcile_classification TEXT
        CHECK (reconcile_classification IN ('STILL_RUNNING', 'FINISHED_NEEDS_CLEANUP', 'ALREADY_CLEAN', 'STALE_EXECUTION', 'ORPHANED_EXECUTION', 'CONTRADICTORY', 'INSUFFICIENT_EVIDENCE')),
    ADD COLUMN reconcile_action TEXT
        CHECK (reconcile_action IN ('KEEP', 'TERMINATE_CLEANUP', 'ATTEST')),
    ADD COLUMN reconcile_evidence JSONB,
    ADD COLUMN reconcile_incarnation BIGINT
        CHECK (reconcile_incarnation IS NULL OR reconcile_incarnation >= 0),
    ADD COLUMN reconcile_seq BIGINT
        CHECK (reconcile_seq IS NULL OR reconcile_seq >= 1),
    ADD COLUMN reconcile_updated_at TIMESTAMPTZ;

-- Bounded background pass finds active work still held by quarantined runners.
CREATE INDEX IF NOT EXISTS ix_allocations_reconcile_pending
    ON allocations (reconcile_requested) WHERE state = 'ACTIVE';
