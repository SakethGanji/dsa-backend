-- 1. Core Security & User Entities
-- (No changes in this section)

CREATE TABLE IF NOT EXISTS roles (
    id SERIAL PRIMARY KEY,
    role_name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    soeid VARCHAR(20) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role_id INT NOT NULL REFERENCES roles(id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 2. Content-Addressable & Deduplicated File Store
-- (No changes in this section)

CREATE TABLE IF NOT EXISTS files (
    id SERIAL PRIMARY KEY,
    storage_type VARCHAR(50) NOT NULL, -- e.g. 's3', 'local'
    file_type VARCHAR(50) NOT NULL, -- e.g. 'parquet','csv','json'
    mime_type VARCHAR(100),
    file_path TEXT, -- e.g. S3 URI
    file_size BIGINT,
    content_hash CHAR(64) UNIQUE, -- SHA256 for dedupe
    reference_count BIGINT NOT NULL DEFAULT 0, -- for safe GC
    compression_type VARCHAR(50), -- e.g. 'snappy','zstd'
    metadata JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 2.1 File-level permissions
CREATE TYPE file_permission AS ENUM ('read','write','admin');

CREATE TABLE IF NOT EXISTS file_permissions (
    file_id INT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    permission_type file_permission NOT NULL,
    PRIMARY KEY (file_id, user_id)
);

-- 3. Datasets
-- --- MODIFIED: Added ON DELETE SET NULL for created_by for data safety.
CREATE TABLE IF NOT EXISTS datasets (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_by INT REFERENCES users(id) ON DELETE SET NULL, -- SAFER: Dataset persists if user is deleted.
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(name, created_by)
);

-- 3.1 Dataset-level permissions
CREATE TYPE dataset_permission AS ENUM ('read','write','admin');

CREATE TABLE IF NOT EXISTS dataset_permissions (
    dataset_id INT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    permission_type dataset_permission NOT NULL,
    PRIMARY KEY (dataset_id, user_id)
);

-- 4. Versioning
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'version_status') THEN
        CREATE TYPE version_status AS ENUM('active','archived','deleted');
    END IF;
END$$;

-- --- MODIFIED: Added ON DELETE SET NULL for created_by for data safety.
CREATE TABLE IF NOT EXISTS dataset_versions (
    id SERIAL PRIMARY KEY,
    dataset_id INT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    overlay_file_id INT NOT NULL REFERENCES files(id),
    message TEXT,
    version_number INT NOT NULL,
    status version_status NOT NULL DEFAULT 'active',
    created_by INT REFERENCES users(id) ON DELETE SET NULL, -- SAFER: Version persists if user is deleted.
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (dataset_id, version_number)
);

CREATE INDEX IF NOT EXISTS idx_latest_version ON dataset_versions (dataset_id, version_number DESC);

-- 4.1 Version Tagging
CREATE TABLE IF NOT EXISTS version_tags (
    id SERIAL PRIMARY KEY,
    dataset_id INT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    tag_name VARCHAR(255) NOT NULL,
    dataset_version_id INT NOT NULL REFERENCES dataset_versions(id) ON DELETE CASCADE,
    UNIQUE (dataset_id, tag_name)
);

CREATE INDEX IF NOT EXISTS idx_version_tags_lookup ON version_tags (dataset_id, tag_name);

-- 5. Multi-file Support
CREATE TABLE IF NOT EXISTS dataset_version_files (
    version_id INT NOT NULL REFERENCES dataset_versions(id) ON DELETE CASCADE,
    file_id INT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    component_type VARCHAR(50) NOT NULL,
    component_name TEXT,
    component_index INT,
    metadata JSONB,
    PRIMARY KEY (version_id, file_id)
);

-- 6. Schema Evolution
CREATE TABLE IF NOT EXISTS dataset_schema_versions (
    id SERIAL PRIMARY KEY,
    dataset_version_id INT NOT NULL REFERENCES dataset_versions(id) ON DELETE CASCADE,
    schema_json JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 7. Dataset Tags (categorization)
CREATE TABLE IF NOT EXISTS tags (
    id SERIAL PRIMARY KEY,
    tag_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dataset_tags (
    dataset_id INT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    tag_id INT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (dataset_id, tag_id)
);

-- 8. Sampling, Exploration & Analysis
-- --- REPLACED: `sampling_runs` and `exploration_runs` are consolidated into `analysis_runs`.

-- (Optional but recommended) A table for reusable configuration templates.
CREATE TABLE IF NOT EXISTS analysis_configurations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    analysis_type VARCHAR(100) NOT NULL, -- e.g., 'sampling', 'profiling'
    parameters JSONB NOT NULL, -- The template parameters
    created_by INT REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(name, created_by)
);

-- The new consolidated table for all analysis runs.
CREATE TYPE analysis_run_type AS ENUM ('sampling', 'exploration', 'profiling');
CREATE TYPE analysis_run_status AS ENUM ('pending', 'running', 'completed', 'failed');

CREATE TABLE IF NOT EXISTS analysis_runs (
    id SERIAL PRIMARY KEY,
    -- Core Links
    dataset_version_id INT NOT NULL REFERENCES dataset_versions(id) ON DELETE CASCADE,
    user_id INT REFERENCES users(id) ON DELETE SET NULL,

    -- Type and Configuration
    run_type analysis_run_type NOT NULL,
    run_parameters JSONB NOT NULL, -- The *actual* parameters used, ensuring reproducibility.
    
    -- (Optional) Link back to a saved configuration template for reference.
    saved_config_id INT REFERENCES analysis_configurations(id) ON DELETE SET NULL,

    -- Execution Metadata
    status analysis_run_status NOT NULL DEFAULT 'pending',
    run_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    execution_time_ms INT,
    notes TEXT,

    -- Outputs
    -- The output sample file (for 'sampling' type). Stored in our main files table.
    output_file_id INT REFERENCES files(id) ON DELETE SET NULL,
    -- The output summary stats (for 'exploration' or 'profiling' types).
    output_summary JSONB
);

CREATE INDEX IF NOT EXISTS idx_analysis_runs_user ON analysis_runs(user_id);
CREATE INDEX IF NOT EXISTS idx_analysis_runs_version ON analysis_runs(dataset_version_id);


-- 9. Dataset Statistics (for monitoring & compaction)
-- (No changes in this section)

CREATE TABLE IF NOT EXISTS dataset_statistics (
    version_id INT PRIMARY KEY REFERENCES dataset_versions(id) ON DELETE CASCADE,
    row_count BIGINT,
    column_count INT,
    size_bytes BIGINT,
    statistics JSONB,
    computed_at TIMESTAMP NOT NULL DEFAULT NOW()
);