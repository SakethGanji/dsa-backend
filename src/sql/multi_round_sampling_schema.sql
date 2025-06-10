-- Multi-round sampling tables

-- Main table for multi-round sampling jobs
CREATE TABLE IF NOT EXISTS multi_round_sampling_jobs (
  id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  dataset_id          INT         NOT NULL REFERENCES datasets(id),
  dataset_version_id  INT         NOT NULL REFERENCES dataset_versions(id),
  user_id             INT         NOT NULL REFERENCES users(id),
  total_rounds        INT         NOT NULL,
  export_residual     BOOLEAN     NOT NULL DEFAULT TRUE,
  residual_output_name VARCHAR(255),
  sheet_name          VARCHAR(255),
  status              VARCHAR(20) NOT NULL DEFAULT 'pending',
  created_at          TIMESTAMP   NOT NULL DEFAULT NOW(),
  started_at          TIMESTAMP,
  completed_at        TIMESTAMP,
  error_message       TEXT,
  
  CONSTRAINT check_status CHECK (status IN ('pending', 'running', 'completed', 'failed')),
  CONSTRAINT check_rounds CHECK (total_rounds > 0)
);

-- Configuration for each round within a multi-round job
CREATE TABLE IF NOT EXISTS multi_round_sampling_rounds (
  id                  SERIAL      PRIMARY KEY,
  job_id              UUID        NOT NULL REFERENCES multi_round_sampling_jobs(id) ON DELETE CASCADE,
  round_number        INT         NOT NULL,
  sampling_method     VARCHAR(100) NOT NULL,
  sampling_parameters JSONB       NOT NULL,
  output_name         VARCHAR(255) NOT NULL,
  filters             JSONB,
  selection           JSONB,
  status              VARCHAR(20) NOT NULL DEFAULT 'pending',
  started_at          TIMESTAMP,
  completed_at        TIMESTAMP,
  sample_size         INT,
  output_file_id      INT         REFERENCES files(id),
  output_uri          TEXT,
  preview_data        JSONB,
  data_summary        JSONB,
  error_message       TEXT,
  
  CONSTRAINT uq_round_per_job UNIQUE (job_id, round_number),
  CONSTRAINT check_round_status CHECK (status IN ('pending', 'running', 'completed', 'failed')),
  CONSTRAINT check_round_number CHECK (round_number > 0)
);

-- Residual tracking for multi-round sampling
CREATE TABLE IF NOT EXISTS multi_round_residuals (
  id                  SERIAL      PRIMARY KEY,
  job_id              UUID        NOT NULL REFERENCES multi_round_sampling_jobs(id) ON DELETE CASCADE,
  after_round         INT         NOT NULL,
  residual_count      INT         NOT NULL,
  residual_file_id    INT         REFERENCES files(id),
  residual_uri        TEXT,
  residual_summary    JSONB,
  created_at          TIMESTAMP   NOT NULL DEFAULT NOW(),
  
  CONSTRAINT uq_residual_per_round UNIQUE (job_id, after_round),
  CONSTRAINT check_after_round CHECK (after_round >= 0) -- 0 means initial dataset
);

-- Index for performance
CREATE INDEX idx_multi_round_jobs_user ON multi_round_sampling_jobs(user_id);
CREATE INDEX idx_multi_round_jobs_status ON multi_round_sampling_jobs(status);
CREATE INDEX idx_multi_round_jobs_dataset ON multi_round_sampling_jobs(dataset_id, dataset_version_id);
CREATE INDEX idx_multi_round_rounds_job ON multi_round_sampling_rounds(job_id);
CREATE INDEX idx_multi_round_rounds_status ON multi_round_sampling_rounds(status);
CREATE INDEX idx_multi_round_residuals_job ON multi_round_residuals(job_id);