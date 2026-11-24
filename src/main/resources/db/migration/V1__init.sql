CREATE TABLE job_definition (
  id            UUID PRIMARY KEY,
  tenant_id     VARCHAR(64)  NOT NULL,
  name          VARCHAR(200) NOT NULL,
  job_type      VARCHAR(32)  NOT NULL,
  payload       JSONB        NOT NULL,
  status        VARCHAR(16)  NOT NULL,
  allow_overlap BOOLEAN      NOT NULL DEFAULT FALSE,
  timeout_ms    INT          NOT NULL DEFAULT 30000,
  max_attempts  INT          NOT NULL DEFAULT 3,
  version       BIGINT       NOT NULL DEFAULT 0,
  created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, name)
);

CREATE TABLE job_trigger (
  id                   UUID PRIMARY KEY,
  job_id               UUID        NOT NULL REFERENCES job_definition(id),
  schedule_type        VARCHAR(16) NOT NULL,
  cron_expr            VARCHAR(120),
  timezone             VARCHAR(40),
  run_at               TIMESTAMPTZ,
  next_fire_time       TIMESTAMPTZ,
  state                VARCHAR(16) NOT NULL,
  misfire_policy       VARCHAR(40) NOT NULL,
  misfire_threshold_ms INT         NOT NULL DEFAULT 5000,
  fire_count           BIGINT      NOT NULL DEFAULT 0,
  promoted             BOOLEAN     NOT NULL DEFAULT FALSE,
  version              BIGINT      NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX uq_trigger_job ON job_trigger (job_id);
CREATE INDEX idx_trigger_due ON job_trigger (next_fire_time)
  WHERE state = 'ACTIVE' AND promoted = FALSE;

CREATE TABLE job_execution_audit (
  id                  UUID PRIMARY KEY,
  job_id              UUID        NOT NULL,
  trigger_id          UUID        NOT NULL,
  scheduled_fire_time TIMESTAMPTZ NOT NULL,
  status              VARCHAR(16) NOT NULL,
  attempt             INT         NOT NULL,
  lease_version       BIGINT      NOT NULL DEFAULT 0,
  worker_id           VARCHAR(64),
  started_at          TIMESTAMPTZ,
  finished_at         TIMESTAMPTZ,
  duration_ms         BIGINT,
  heartbeat_at        TIMESTAMPTZ,
  lease_until         TIMESTAMPTZ,
  output              JSONB,
  error_stack         TEXT,
  CONSTRAINT uq_exec_fire UNIQUE (trigger_id, scheduled_fire_time, attempt)
);
CREATE INDEX idx_exec_job ON job_execution_audit (job_id, scheduled_fire_time DESC);
CREATE INDEX idx_exec_lease ON job_execution_audit (lease_until) WHERE status = 'RUNNING';
