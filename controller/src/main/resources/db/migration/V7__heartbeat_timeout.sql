-- V7 heartbeat timeout and contact tracking (issue #26).
-- Heartbeat loss / contact loss quarantines runners without resolving execution or releasing ownership.
ALTER TABLE allocations
    ADD COLUMN last_contact_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN heartbeat_timeout_ms BIGINT NOT NULL DEFAULT 15000
        CHECK (heartbeat_timeout_ms BETWEEN 1 AND 86400000);

-- Backfill last_contact_at to created_at for pre-existing rows.
UPDATE allocations SET last_contact_at = created_at;

-- Dedicated partial index for active allocation contact evaluation.
CREATE INDEX IF NOT EXISTS ix_allocations_active_contact
    ON allocations (last_contact_at) WHERE state = 'ACTIVE';

