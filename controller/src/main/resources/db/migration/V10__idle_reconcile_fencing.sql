-- V10 idle reconcile sequencing (issue #33 follow-up).
--
-- Allocated RECONCILE is fenced by allocations.max_seq (seq must increase,
-- duplicates return dropped_stale with zero mutation). Idle-at-backup RECONCILE
-- has no allocation row, so the same guarantee needs per-runner state.
-- runners.idle_reconcile_seq is the last accepted idle RECONCILE seq for
-- quarantined idle runners (NULL means never observed). Idle RECONCILE with
-- seq <= idle_reconcile_seq is rejected as dropped_stale with zero mutation,
-- including no quarantine creation/clearing. AVAILABLE fast-paths perform no
-- writes and need no sequencing.
ALTER TABLE runners ADD COLUMN idle_reconcile_seq BIGINT
    CHECK (idle_reconcile_seq IS NULL OR idle_reconcile_seq >= 1);
