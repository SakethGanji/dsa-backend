CREATE TABLE IF NOT EXISTS roles (
  id          SERIAL      PRIMARY KEY,
  role_name   VARCHAR(50)  UNIQUE NOT NULL,
  description TEXT
);

CREATE TABLE IF NOT EXISTS users (
  id             SERIAL      PRIMARY KEY,
  soeid          VARCHAR(20)  UNIQUE NOT NULL,
  password_hash  VARCHAR(255) NOT NULL,
  role_id        INT          NOT NULL REFERENCES roles(id),
  created_at     TIMESTAMP    NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS files (
  id            SERIAL      PRIMARY KEY,
  storage_type  VARCHAR(50)  NOT NULL,
  file_type     VARCHAR(50)  NOT NULL,
  mime_type     VARCHAR(100),
  file_data     BYTEA,
  file_path     TEXT,
  file_size     BIGINT,
  created_at    TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS datasets (
  id           SERIAL      PRIMARY KEY,
  name         VARCHAR(255) NOT NULL,
  description  TEXT,
  created_by   INT          NOT NULL REFERENCES users(id),
  created_at   TIMESTAMP    NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dataset_versions (
  id                       SERIAL      PRIMARY KEY,
  dataset_id               INT         NOT NULL REFERENCES datasets(id),
  version_number           INT         NOT NULL,
  file_id                  INT         NOT NULL REFERENCES files(id),
  ingestion_timestamp      TIMESTAMP    NOT NULL DEFAULT NOW(),
  last_updated_timestamp   TIMESTAMP    NOT NULL DEFAULT NOW(),
  uploaded_by              INT         NOT NULL REFERENCES users(id),
  parent_version_id        INT         REFERENCES dataset_versions(id),
  message                  TEXT,
  overlay_file_id          INT         REFERENCES files(id),
  UNIQUE (dataset_id, version_number)
);

-- Index for parent version lookups
CREATE INDEX IF NOT EXISTS idx_dataset_versions_parent ON dataset_versions(parent_version_id);

CREATE TABLE IF NOT EXISTS sheets (
  id                   SERIAL      PRIMARY KEY,
  dataset_version_id   INT         NOT NULL REFERENCES dataset_versions(id),
  name                 VARCHAR(255) NOT NULL,
  sheet_index          INT         NOT NULL,
  description          TEXT,
  UNIQUE (dataset_version_id, sheet_index)
);

-- METADATA tables (tags + sheet metadata)

CREATE TABLE IF NOT EXISTS tags (
  id          SERIAL      PRIMARY KEY,
  name        VARCHAR(100) UNIQUE NOT NULL,
  description TEXT
);

CREATE TABLE IF NOT EXISTS dataset_tags (
  dataset_id  INT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
  tag_id      INT NOT NULL REFERENCES tags(id)     ON DELETE RESTRICT,
  PRIMARY KEY (dataset_id, tag_id)
);

CREATE TABLE IF NOT EXISTS sheet_metadata (
  id                       SERIAL      PRIMARY KEY,
  sheet_id                 INT         NOT NULL REFERENCES sheets(id)   ON DELETE CASCADE,
  metadata                 JSONB       NOT NULL,
  profiling_report_file_id INT         REFERENCES files(id)            ON DELETE SET NULL
);

-- EXPERIMENTS tables

CREATE TABLE IF NOT EXISTS data_exploration_configs (
  id         SERIAL      PRIMARY KEY,
  user_id    INT          NOT NULL REFERENCES users(id),
  name       VARCHAR(255) NOT NULL,
  config     JSONB       NOT NULL,
  created_at TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS data_exploration_runs (
  id                 SERIAL      PRIMARY KEY,
  dataset_version_id INT         NOT NULL REFERENCES dataset_versions(id),
  sheet_id           INT            REFERENCES sheets(id),
  user_id            INT         NOT NULL REFERENCES users(id),
  exploration_config JSONB       NOT NULL,
  run_timestamp      TIMESTAMP    NOT NULL DEFAULT NOW(),
  output_summary     JSONB
);

CREATE TABLE IF NOT EXISTS sampling_runs (
  id                  SERIAL      PRIMARY KEY,
  dataset_version_id  INT         NOT NULL REFERENCES dataset_versions(id),
  sheet_id            INT            REFERENCES sheets(id),
  user_id             INT         NOT NULL REFERENCES users(id),
  sampling_method     VARCHAR(100) NOT NULL,
  sampling_parameters JSONB,
  run_timestamp       TIMESTAMP    NOT NULL DEFAULT NOW(),
  status              VARCHAR(20)
);

-- OUTPUTS

-- sampling outputs
CREATE TABLE IF NOT EXISTS sampling_outputs (
  id                 SERIAL      PRIMARY KEY,
  sampling_run_id    INT         NOT NULL REFERENCES sampling_runs(id) ON DELETE CASCADE,
  output_index       INT         NOT NULL,
  file_id            INT         NOT NULL REFERENCES files(id)        ON DELETE RESTRICT,
  created_by         INT         NOT NULL REFERENCES users(id),
  created_at         TIMESTAMP    NOT NULL DEFAULT NOW(),
  output_name        VARCHAR(255),
  metadata           JSONB,
  CONSTRAINT uq_sampling_output_per_run UNIQUE (sampling_run_id, output_index)
);

-- exploration outputs
CREATE TABLE IF NOT EXISTS exploration_outputs (
  id                    SERIAL      PRIMARY KEY,
  exploration_run_id    INT         NOT NULL REFERENCES data_exploration_runs(id) ON DELETE CASCADE,
  output_index          INT         NOT NULL,
  file_id               INT         NOT NULL REFERENCES files(id)               ON DELETE RESTRICT,
  created_by            INT         NOT NULL REFERENCES users(id),
  created_at            TIMESTAMP    NOT NULL DEFAULT NOW(),
  metadata              JSONB,
  CONSTRAINT uq_exploration_output_per_run UNIQUE (exploration_run_id, output_index)
);

-- SCHEMA CAPTURE

-- Schema snapshots for dataset versions
CREATE TABLE IF NOT EXISTS dataset_schema_versions (
  id                    SERIAL      PRIMARY KEY,
  dataset_version_id    INT         NOT NULL REFERENCES dataset_versions(id),
  schema_json           JSONB       NOT NULL,
  created_at            TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Index for schema version lookups
CREATE INDEX IF NOT EXISTS idx_schema_versions_dataset ON dataset_schema_versions(dataset_version_id);

-- MULTI-FILE SUPPORT

-- Junction table for version-file relationships
CREATE TABLE IF NOT EXISTS dataset_version_files (
  version_id        INT         NOT NULL REFERENCES dataset_versions(id),
  file_id           INT         NOT NULL REFERENCES files(id),
  component_type    VARCHAR(50) NOT NULL,
  component_name    TEXT,
  component_index   INT,
  metadata          JSONB,
  PRIMARY KEY (version_id, file_id)
);

-- Index for version file lookups
CREATE INDEX IF NOT EXISTS idx_version_files_version ON dataset_version_files(version_id);

-- BRANCH AND TAG SUPPORT

-- Pointers table for branches and tags
CREATE TABLE IF NOT EXISTS dataset_pointers (
  id                  SERIAL       PRIMARY KEY,
  dataset_id          INT          NOT NULL REFERENCES datasets(id),
  pointer_name        VARCHAR(255) NOT NULL,
  dataset_version_id  INT          NOT NULL REFERENCES dataset_versions(id),
  is_tag              BOOLEAN      NOT NULL DEFAULT FALSE,
  created_at          TIMESTAMP    NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMP    NOT NULL DEFAULT NOW(),
  UNIQUE (dataset_id, pointer_name)
);

-- Index for dataset pointer lookups
CREATE INDEX IF NOT EXISTS idx_pointers_dataset ON dataset_pointers(dataset_id);

-- PERMISSIONS

-- Permissions table for dataset and file access control
CREATE TABLE IF NOT EXISTS permissions (
  id                SERIAL      PRIMARY KEY,
  resource_type     VARCHAR(50) NOT NULL, -- 'dataset' or 'file'
  resource_id       INT         NOT NULL,
  user_id           INT         NOT NULL REFERENCES users(id),
  permission_type   VARCHAR(20) NOT NULL, -- 'read', 'write', 'admin'
  granted_at        TIMESTAMP   NOT NULL DEFAULT NOW(),
  granted_by        INT         NOT NULL REFERENCES users(id),
  UNIQUE (resource_type, resource_id, user_id, permission_type)
);

-- Index for permission lookups
CREATE INDEX IF NOT EXISTS idx_permissions_lookup ON permissions(resource_type, resource_id, user_id);