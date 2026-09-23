-- Execution results are independent of allocation release and physical safe reuse.
ALTER TABLE jobs
    ADD COLUMN result TEXT CHECK (result IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')),
    ADD COLUMN cancel_requested BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE attempts
    ADD COLUMN result TEXT CHECK (result IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT'));
ALTER TABLE runners ADD COLUMN quarantine_reason TEXT;
ALTER TABLE allocations
    DROP CONSTRAINT chk_allocations_state,
    DROP CONSTRAINT allocations_report_status_check,
    ADD CONSTRAINT chk_allocations_state CHECK (state IN ('ACTIVE', 'RELEASED')),
    ADD CONSTRAINT allocations_report_status_check
        CHECK (report_status IN ('STARTING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')),
    ADD COLUMN workload_timeout_ms BIGINT NOT NULL DEFAULT 3600000
        CHECK (workload_timeout_ms BETWEEN 1 AND 86400000),
    ADD COLUMN cleanup_evidence JSONB,
    ADD COLUMN released_at TIMESTAMPTZ;
