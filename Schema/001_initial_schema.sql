CREATE TABLE IF NOT EXISTS workflows (
    id TEXT PRIMARY KEY,
    state TEXT NOT NULL DEFAULT 'SUBMITTED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    workflow_id TEXT REFERENCES workflows(id),
    priority SMALLINT NOT NULL CHECK (priority BETWEEN 0 AND 100),
    cpu_millis BIGINT NOT NULL CHECK (cpu_millis >= 0),
    memory_bytes NUMERIC(20, 0) NOT NULL CHECK (memory_bytes >= 0),
    gpu_count BIGINT NOT NULL CHECK (gpu_count >= 0),
    deadline_unix_ms BIGINT,
    state TEXT NOT NULL,
    version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS jobs_state_priority_idx
    ON jobs (state, priority DESC, created_at);

CREATE TABLE IF NOT EXISTS job_dependencies (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    prerequisite_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, prerequisite_id),
    CHECK (job_id <> prerequisite_id)
);

CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,
    cpu_capacity BIGINT NOT NULL CHECK (cpu_capacity >= 0),
    memory_capacity NUMERIC(20, 0) NOT NULL CHECK (memory_capacity >= 0),
    gpu_capacity BIGINT NOT NULL CHECK (gpu_capacity >= 0),
    cpu_allocated BIGINT NOT NULL CHECK (cpu_allocated >= 0),
    memory_allocated NUMERIC(20, 0) NOT NULL CHECK (memory_allocated >= 0),
    gpu_allocated BIGINT NOT NULL CHECK (gpu_allocated >= 0),
    healthy BOOLEAN NOT NULL,
    last_heartbeat TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS job_attempts (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_number BIGINT NOT NULL CHECK (attempt_number > 0),
    worker_id TEXT REFERENCES workers(id),
    state TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    error TEXT,
    PRIMARY KEY (job_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS job_events (
    event_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    previous_state TEXT NOT NULL,
    new_state TEXT NOT NULL,
    version BIGINT NOT NULL,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (job_id, version)
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id BIGSERIAL PRIMARY KEY,
    job_id TEXT REFERENCES jobs(id) ON DELETE SET NULL,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS job_attempts_job_idx
    ON job_attempts (job_id, attempt_number DESC);
CREATE INDEX IF NOT EXISTS audit_logs_job_created_idx
    ON audit_logs (job_id, created_at DESC);
