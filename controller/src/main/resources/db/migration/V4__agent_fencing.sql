-- Agent process fencing and report sequencing; ownership remains ACTIVE until
-- positive cleanup proof is implemented. Poll never creates an allocation.
ALTER TABLE runners ADD COLUMN agent_incarnation BIGINT
    CHECK (agent_incarnation >= 0);

ALTER TABLE allocations
    ADD COLUMN agent_incarnation BIGINT CHECK (agent_incarnation >= 0),
    ADD COLUMN max_seq BIGINT NOT NULL DEFAULT 0 CHECK (max_seq >= 0),
    ADD COLUMN report_status TEXT
        CHECK (report_status IN ('STARTING', 'RUNNING', 'SUCCEEDED', 'FAILED'));
