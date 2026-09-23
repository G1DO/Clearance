-- Interrupted execution is an attempt disposition, never a logical job result.
ALTER TABLE attempts
    DROP CONSTRAINT attempts_result_check,
    ADD CONSTRAINT attempts_result_check
        CHECK (result IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT', 'INTERRUPTED')),
    ADD COLUMN recovery_reason TEXT;

ALTER TABLE allocations
    DROP CONSTRAINT allocations_report_status_check,
    ADD CONSTRAINT allocations_report_status_check
        CHECK (report_status IN ('STARTING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT', 'INTERRUPTED')),
    ADD COLUMN discovery_required BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN recovery_action TEXT CHECK (recovery_action IN ('TERMINATE', 'QUARANTINE')),
    ADD COLUMN recovery_evidence JSONB,
    ADD COLUMN recovery_incarnation BIGINT CHECK (recovery_incarnation >= 0),
    ADD COLUMN retry_allocation_id UUID REFERENCES allocations (allocation_id);
