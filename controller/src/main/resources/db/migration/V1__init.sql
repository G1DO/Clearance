-- V1 bootstrap baseline for issue #4.
-- Proves Flyway migrates an empty database.
-- Full project-scoped operations/jobs schema lands in the next todo.
CREATE TABLE IF NOT EXISTS bootstrap_check (
    id BIGINT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
