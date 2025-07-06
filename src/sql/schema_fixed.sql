-- =============================================================================
--          DSA PLATFORM v2 - FIXED SCHEMA (No Circular Dependencies)
--
-- This schema uses a multi-schema architecture for improved security,
-- organization, and long-term maintainability.
--
-- SCHEMAS:
--   dsa_auth: Manages users, roles, and permissions.
--   dsa_core: The core data model, including the Git-like versioning engine.
--   dsa_jobs: The asynchronous job queue for analysis and processing.
-- =============================================================================

-- Drop schemas in reverse dependency order if needed for clean rebuild
-- DROP SCHEMA IF EXISTS dsa_jobs CASCADE;
-- DROP SCHEMA IF EXISTS dsa_core CASCADE;
-- DROP SCHEMA IF EXISTS dsa_auth CASCADE;

-- REQUIRED: Create the logical schemas for the platform.
CREATE SCHEMA IF NOT EXISTS dsa_auth;
CREATE SCHEMA IF NOT EXISTS dsa_core;
CREATE SCHEMA IF NOT EXISTS dsa_jobs;

-- REQUIRED: Enable cryptographic functions (these are database-wide extensions).
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


--=============================================================================
-- 1. AUTHENTICATION SCHEMA (`dsa_auth`) - Base tables only
--=============================================================================

CREATE TABLE IF NOT EXISTS dsa_auth.roles (
    id SERIAL PRIMARY KEY,
    role_name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS dsa_auth.users (
    id SERIAL PRIMARY KEY,
    soeid VARCHAR(20) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role_id INT NOT NULL REFERENCES dsa_auth.roles(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create the permission type but don't create the permissions table yet
DO $$ BEGIN
    CREATE TYPE dsa_auth.dataset_permission AS ENUM ('read','write','admin');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;


--=============================================================================
-- 2. CORE DATA MODEL & VERSIONING ENGINE SCHEMA (`dsa_core`)
--=============================================================================

CREATE TABLE IF NOT EXISTS dsa_core.datasets (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_by INT REFERENCES dsa_auth.users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(name, created_by)
);

-- Content-addressable store for every unique row.
CREATE TABLE IF NOT EXISTS dsa_core.rows (
    row_hash CHAR(64) PRIMARY KEY, -- SHA256 hash of the canonicalized 'data' field.
    data JSONB NOT NULL
);
COMMENT ON TABLE dsa_core.rows IS 'Content-addressable store for all unique data rows (blobs).';


-- Models the version graph (Directed Acyclic Graph).
CREATE TABLE IF NOT EXISTS dsa_core.commits (
    commit_id CHAR(64) PRIMARY KEY,
    dataset_id INT NOT NULL REFERENCES dsa_core.datasets(id) ON DELETE CASCADE,
    parent_commit_id CHAR(64) REFERENCES dsa_core.commits(commit_id),
    message TEXT,
    author_id INT REFERENCES dsa_auth.users(id) ON DELETE SET NULL,
    authored_at TIMESTAMPTZ,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE dsa_core.commits IS 'An immutable, point-in-time snapshot of a dataset.';
CREATE INDEX IF NOT EXISTS idx_commits_dataset_id ON dsa_core.commits(dataset_id);


-- The manifest linking a commit to its constituent rows.
CREATE TABLE IF NOT EXISTS dsa_core.commit_rows (
    commit_id CHAR(64) NOT NULL REFERENCES dsa_core.commits(commit_id) ON DELETE CASCADE,
    logical_row_id TEXT NOT NULL,
    row_hash CHAR(64) NOT NULL REFERENCES dsa_core.rows(row_hash),
    PRIMARY KEY (commit_id, logical_row_id)
);
COMMENT ON TABLE dsa_core.commit_rows IS 'The manifest linking a commit to its constituent rows.';
CREATE INDEX IF NOT EXISTS idx_commit_rows_row_hash ON dsa_core.commit_rows(row_hash);


-- Named pointers (branches, tags) to specific commits.
CREATE TABLE IF NOT EXISTS dsa_core.refs (
    id SERIAL PRIMARY KEY,
    dataset_id INT NOT NULL REFERENCES dsa_core.datasets(id) ON DELETE CASCADE,
    name TEXT NOT NULL, -- e.g., "main", "v1.2-approved", "staging_sample"
    commit_id CHAR(64) REFERENCES dsa_core.commits(commit_id) ON DELETE CASCADE,  -- Can be NULL initially
    UNIQUE (dataset_id, name)
);
COMMENT ON TABLE dsa_core.refs IS 'Named pointers (branches, tags) to specific commits.';


-- Schema Evolution & Dataset Categorization
CREATE TABLE IF NOT EXISTS dsa_core.commit_schemas (
    id SERIAL PRIMARY KEY,
    commit_id CHAR(64) NOT NULL REFERENCES dsa_core.commits(commit_id) ON DELETE CASCADE UNIQUE,
    schema_definition JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE dsa_core.commit_schemas IS 'Stores the schema definition(s) for a specific commit.';

CREATE TABLE IF NOT EXISTS dsa_core.tags (
    id SERIAL PRIMARY KEY,
    tag_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dsa_core.dataset_tags (
    dataset_id INT NOT NULL REFERENCES dsa_core.datasets(id) ON DELETE CASCADE,
    tag_id INT NOT NULL REFERENCES dsa_core.tags(id) ON DELETE CASCADE,
    PRIMARY KEY (dataset_id, tag_id)
);

-- Cached statistics for a commit (generated by a job, but describes a core entity)
CREATE TABLE IF NOT EXISTS dsa_core.commit_statistics (
    commit_id CHAR(64) PRIMARY KEY REFERENCES dsa_core.commits(commit_id) ON DELETE CASCADE,
    row_count BIGINT,
    size_bytes BIGINT,
    statistics JSONB,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE dsa_core.commit_statistics IS 'Cached aggregate statistics for the data within a specific commit.';


--=============================================================================
-- 3. JOBS & ANALYSIS SCHEMA (`dsa_jobs`)
--=============================================================================

CREATE TABLE IF NOT EXISTS dsa_jobs.analysis_configurations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    analysis_type VARCHAR(100) NOT NULL,
    parameters JSONB NOT NULL,
    created_by INT REFERENCES dsa_auth.users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(name, created_by)
);

DO $$ BEGIN
    CREATE TYPE dsa_jobs.analysis_run_type AS ENUM ('import', 'sampling', 'exploration', 'profiling');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE dsa_jobs.analysis_run_status AS ENUM ('pending', 'running', 'completed', 'failed');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;


-- The master job queue table.
CREATE TABLE IF NOT EXISTS dsa_jobs.analysis_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_type dsa_jobs.analysis_run_type NOT NULL,
    status dsa_jobs.analysis_run_status NOT NULL DEFAULT 'pending',

    -- Links to other schemas for context and permissions checks.
    dataset_id INT NOT NULL REFERENCES dsa_core.datasets(id) ON DELETE CASCADE,
    source_commit_id CHAR(64) REFERENCES dsa_core.commits(commit_id) ON DELETE CASCADE,
    user_id INT REFERENCES dsa_auth.users(id) ON DELETE SET NULL,

    run_parameters JSONB,
    output_summary JSONB,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);
COMMENT ON TABLE dsa_jobs.analysis_runs IS 'The master job queue for all asynchronous operations.';

-- An optimized index for the background worker to find pending jobs.
CREATE INDEX IF NOT EXISTS idx_analysis_runs_pending_jobs ON dsa_jobs.analysis_runs(status, run_type) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_analysis_runs_dataset_id ON dsa_jobs.analysis_runs(dataset_id);


--=============================================================================
-- 4. CROSS-SCHEMA TABLES (Created after all base schemas exist)
--=============================================================================

-- NOW we can create the dataset_permissions table that bridges auth and core schemas
CREATE TABLE IF NOT EXISTS dsa_auth.dataset_permissions (
    dataset_id INT NOT NULL REFERENCES dsa_core.datasets(id) ON DELETE CASCADE,
    user_id INT NOT NULL REFERENCES dsa_auth.users(id) ON DELETE CASCADE,
    permission_type dsa_auth.dataset_permission NOT NULL,
    PRIMARY KEY (dataset_id, user_id)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_dataset_permissions_user_id ON dsa_auth.dataset_permissions(user_id);
CREATE INDEX IF NOT EXISTS idx_dataset_permissions_dataset_id ON dsa_auth.dataset_permissions(dataset_id);


--=============================================================================
-- 5. INITIAL DATA SETUP
--=============================================================================

-- Insert default roles
INSERT INTO dsa_auth.roles (role_name, description) VALUES
    ('admin', 'Full system administrator'),
    ('analyst', 'Data analyst with read/write permissions'),
    ('viewer', 'Read-only access')
ON CONFLICT (role_name) DO NOTHING;


--=============================================================================
--                                USAGE NOTES
--=============================================================================
-- 1. QUERIES: All application queries must now use fully-qualified table names
--    (e.g., `SELECT * FROM dsa_auth.users;`). Alternatively, the application's
--    database connection can set the search_path:
--    `SET search_path TO dsa_core, dsa_jobs, dsa_auth, public;`
--
-- 2. PERMISSIONS: This structure allows for granular role-based access control.
--    For example, a background worker role can be granted permissions only on
--    the `dsa_jobs` schema, enhancing security.
--    `GRANT USAGE ON SCHEMA dsa_jobs TO worker_role;`
--    `GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA dsa_jobs TO worker_role;`
--=============================================================================