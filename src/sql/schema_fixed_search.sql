-- =============================================================================
--          DSA PLATFORM v2 - SCHEMA with INTEGRATED SEARCH
--
-- This schema uses a multi-schema architecture for improved security,
-- organization, and long-term maintainability.
--
-- SCHEMAS:
--   dsa_auth: Manages users, roles, and permissions.
--   dsa_core: The core data model, including the Git-like versioning engine.
--   dsa_jobs: The asynchronous job queue for analysis and processing.
--   dsa_search: Denormalized views and functions for performant search.
-- =============================================================================

-- Drop schemas in reverse dependency order if needed for clean rebuild
-- DROP SCHEMA IF EXISTS dsa_search CASCADE;
-- DROP SCHEMA IF EXISTS dsa_jobs CASCADE;
-- DROP SCHEMA IF EXISTS dsa_core CASCADE;
-- DROP SCHEMA IF EXISTS dsa_auth CASCADE;

-- REQUIRED: Create the logical schemas for the platform.
CREATE SCHEMA IF NOT EXISTS dsa_auth;
CREATE SCHEMA IF NOT EXISTS dsa_core;
CREATE SCHEMA IF NOT EXISTS dsa_jobs;
CREATE SCHEMA IF NOT EXISTS dsa_search;

-- REQUIRED: Enable cryptographic and utility functions (database-wide).
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- REQUIRED FOR SEARCH: Enable extensions for fuzzy/full-text search.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;


--=============================================================================
-- 1. AUTHENTICATION SCHEMA (`dsa_auth`)
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

DO $$ BEGIN
    CREATE TYPE dsa_auth.dataset_permission AS ENUM ('read','write','admin');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;


--=============================================================================
-- 2. CORE DATA MODEL SCHEMA (`dsa_core`)
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

CREATE TABLE IF NOT EXISTS dsa_core.rows (
    row_hash CHAR(64) PRIMARY KEY,
    data JSONB NOT NULL
);
COMMENT ON TABLE dsa_core.rows IS 'Content-addressable store for all unique data rows (blobs).';

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

CREATE TABLE IF NOT EXISTS dsa_core.commit_rows (
    commit_id CHAR(64) NOT NULL REFERENCES dsa_core.commits(commit_id) ON DELETE CASCADE,
    logical_row_id TEXT NOT NULL,
    row_hash CHAR(64) NOT NULL REFERENCES dsa_core.rows(row_hash),
    PRIMARY KEY (commit_id, logical_row_id)
);
COMMENT ON TABLE dsa_core.commit_rows IS 'The manifest linking a commit to its constituent rows.';
CREATE INDEX IF NOT EXISTS idx_commit_rows_row_hash ON dsa_core.commit_rows(row_hash);

CREATE TABLE IF NOT EXISTS dsa_core.refs (
    id SERIAL PRIMARY KEY,
    dataset_id INT NOT NULL REFERENCES dsa_core.datasets(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    commit_id CHAR(64) REFERENCES dsa_core.commits(commit_id) ON DELETE CASCADE,
    UNIQUE (dataset_id, name)
);
COMMENT ON TABLE dsa_core.refs IS 'Named pointers (branches, tags) to specific commits.';

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

CREATE TABLE IF NOT EXISTS dsa_jobs.analysis_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_type dsa_jobs.analysis_run_type NOT NULL,
    status dsa_jobs.analysis_run_status NOT NULL DEFAULT 'pending',
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
CREATE INDEX IF NOT EXISTS idx_analysis_runs_pending_jobs ON dsa_jobs.analysis_runs(status, run_type) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_analysis_runs_dataset_id ON dsa_jobs.analysis_runs(dataset_id);


--=============================================================================
-- 4. CROSS-SCHEMA TABLES
--=============================================================================

CREATE TABLE IF NOT EXISTS dsa_auth.dataset_permissions (
    dataset_id INT NOT NULL REFERENCES dsa_core.datasets(id) ON DELETE CASCADE,
    user_id INT NOT NULL REFERENCES dsa_auth.users(id) ON DELETE CASCADE,
    permission_type dsa_auth.dataset_permission NOT NULL,
    PRIMARY KEY (dataset_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_dataset_permissions_user_id ON dsa_auth.dataset_permissions(user_id);
CREATE INDEX IF NOT EXISTS idx_dataset_permissions_dataset_id ON dsa_auth.dataset_permissions(dataset_id);


--=============================================================================
-- 5. SEARCH SCHEMA (`dsa_search`)
--=============================================================================
-- This schema provides denormalized views and functions for fast, full-text
-- search across datasets, respecting user permissions.

-- A Materialized View to denormalize dataset information for fast searching.
CREATE MATERIALIZED VIEW IF NOT EXISTS dsa_search.datasets_summary AS
WITH dataset_tags_agg AS (
    SELECT
        dt.dataset_id,
        array_agg(t.tag_name ORDER BY t.tag_name) AS tags
    FROM dsa_core.dataset_tags dt
    JOIN dsa_core.tags t ON dt.tag_id = t.id
    GROUP BY dt.dataset_id
)
SELECT
    d.id AS dataset_id,
    d.name,
    d.description,
    d.created_by AS created_by_id,
    u.soeid AS created_by_name,
    d.created_at,
    d.updated_at,
    COALESCE(dta.tags, '{}'::text[]) AS tags,
    -- Concatenated text for trigram (fuzzy) search
    d.name || ' ' || COALESCE(d.description, '') || ' ' || array_to_string(COALESCE(dta.tags, '{}'), ' ') AS search_text,
    -- TSVector for optimized full-text search
    to_tsvector('english', d.name) ||
    to_tsvector('english', COALESCE(d.description, '')) ||
    to_tsvector('english', array_to_string(COALESCE(dta.tags, '{}'), ' ')) AS search_tsv
FROM dsa_core.datasets d
LEFT JOIN dsa_auth.users u ON d.created_by = u.id
LEFT JOIN dataset_tags_agg dta ON d.id = dta.dataset_id;

COMMENT ON MATERIALIZED VIEW dsa_search.datasets_summary IS
'Denormalized, pre-aggregated dataset data for high-performance searching.';


-- Indexes for the Materialized View
CREATE UNIQUE INDEX IF NOT EXISTS idx_datasets_summary_id ON dsa_search.datasets_summary(dataset_id);
CREATE INDEX IF NOT EXISTS idx_datasets_summary_search_text_trgm ON dsa_search.datasets_summary USING gin (search_text gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_datasets_summary_search_tsv ON dsa_search.datasets_summary USING gin (search_tsv);
CREATE INDEX IF NOT EXISTS idx_datasets_summary_tags ON dsa_search.datasets_summary USING gin (tags);
CREATE INDEX IF NOT EXISTS idx_datasets_summary_created_at ON dsa_search.datasets_summary(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_datasets_summary_updated_at ON dsa_search.datasets_summary(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_datasets_summary_created_by_id ON dsa_search.datasets_summary(created_by_id);


-- Search Function: The primary engine for finding datasets.
CREATE OR REPLACE FUNCTION dsa_search.search(
    p_user_id INT,
    p_filters JSONB
)
RETURNS JSONB AS $$
DECLARE
    -- Timing and query parameters
    start_time TIMESTAMPTZ;
    execution_time_ms INT;
    v_query TEXT := p_filters->>'query';
    v_fuzzy BOOLEAN := COALESCE((p_filters->>'fuzzy')::BOOLEAN, true);
    v_tags TEXT[] := (SELECT array_agg(value) FROM jsonb_array_elements_text(p_filters->'tags'));
    v_created_by INT[] := (SELECT array_agg(value::int) FROM jsonb_array_elements_text(p_filters->'created_by'));
    v_created_after TIMESTAMPTZ := p_filters->>'created_after';
    v_created_before TIMESTAMPTZ := p_filters->>'created_before';
    v_updated_after TIMESTAMPTZ := p_filters->>'updated_after';
    v_updated_before TIMESTAMPTZ := p_filters->>'updated_before';
    v_limit INT := COALESCE((p_filters->>'limit')::INT, 20);
    v_offset INT := COALESCE((p_filters->>'offset')::INT, 0);
    v_sort_by TEXT := COALESCE(p_filters->>'sort_by', 'relevance');
    v_sort_order TEXT := COALESCE(p_filters->>'sort_order', 'desc');
    v_include_facets BOOLEAN := COALESCE((p_filters->>'include_facets')::BOOLEAN, true);
    -- Dynamic SQL parts
    where_clause TEXT;
    order_by_clause TEXT;
    relevance_score TEXT := '0';
BEGIN
    start_time := clock_timestamp();

    -- Build WHERE clause dynamically
    where_clause := 'WHERE dp.user_id = ' || p_user_id;

    IF v_query IS NOT NULL AND v_query != '' THEN
        IF v_fuzzy THEN
            relevance_score := 'similarity(ds.search_text, ' || quote_literal(v_query) || ')';
            where_clause := where_clause || ' AND ' || relevance_score || ' > 0.1';
        ELSE
            relevance_score := 'ts_rank(ds.search_tsv, websearch_to_tsquery(''english'', ' || quote_literal(v_query) || '))';
            where_clause := where_clause || ' AND ds.search_tsv @@ websearch_to_tsquery(''english'', ' || quote_literal(v_query) || ')';
        END IF;
    END IF;

    IF v_tags IS NOT NULL THEN
        where_clause := where_clause || ' AND ds.tags @> ' || quote_literal(v_tags);
    END IF;
    IF v_created_by IS NOT NULL THEN
        where_clause := where_clause || ' AND ds.created_by_id = ANY(' || quote_literal(v_created_by) || ')';
    END IF;
    IF v_created_after IS NOT NULL THEN
        where_clause := where_clause || ' AND ds.created_at >= ' || quote_literal(v_created_after);
    END IF;
    IF v_created_before IS NOT NULL THEN
        where_clause := where_clause || ' AND ds.created_at <= ' || quote_literal(v_created_before);
    END IF;
    IF v_updated_after IS NOT NULL THEN
        where_clause := where_clause || ' AND ds.updated_at >= ' || quote_literal(v_updated_after);
    END IF;
    IF v_updated_before IS NOT NULL THEN
        where_clause := where_clause || ' AND ds.updated_at <= ' || quote_literal(v_updated_before);
    END IF;

    -- Build ORDER BY clause dynamically
    order_by_clause := 'ORDER BY ' ||
        CASE v_sort_by
            WHEN 'relevance' THEN 'score'
            WHEN 'name' THEN 'ds.name'
            WHEN 'created_at' THEN 'ds.created_at'
            WHEN 'updated_at' THEN 'ds.updated_at'
            ELSE 'score'
        END ||
        CASE WHEN v_sort_order = 'asc' THEN ' ASC' ELSE ' DESC' END;

    RETURN (
        WITH filtered_results AS (
            SELECT
                ds.*,
                dp.permission_type,
                (SELECT relevance_score_val FROM (SELECT eval(relevance_score)) AS t(relevance_score_val)) as score
            FROM dsa_search.datasets_summary ds
            -- CRITICAL: Join with permissions to ensure security
            JOIN dsa_auth.dataset_permissions dp ON ds.dataset_id = dp.dataset_id
            WHERE eval(where_clause) -- Use custom function to evaluate dynamic WHERE
        ),
        result_set AS (
            SELECT
                jsonb_build_object(
                    'id', r.dataset_id,
                    'name', r.name,
                    'description', r.description,
                    'created_by', r.created_by_id,
                    'created_by_name', r.created_by_name,
                    'created_at', r.created_at,
                    'updated_at', r.updated_at,
                    'tags', r.tags,
                    'score', r.score,
                    'user_permission', r.permission_type
                ) AS result_json
            FROM filtered_results r
            ORDER BY eval(order_by_clause) -- Use custom function to evaluate dynamic ORDER BY
            LIMIT v_limit
            OFFSET v_offset
        ),
        facets AS (
            SELECT
                CASE WHEN v_include_facets THEN
                    jsonb_build_object(
                        'tags', (
                            SELECT jsonb_object_agg(tag, count)
                            FROM (
                                SELECT unnest(tags) as tag, count(*)
                                FROM filtered_results
                                GROUP BY tag
                                ORDER BY count DESC
                                LIMIT 20
                            ) t
                        ),
                        'created_by', (
                            SELECT jsonb_object_agg(created_by_name, count)
                            FROM (
                                SELECT created_by_name, count(*)
                                FROM filtered_results
                                GROUP BY created_by_name
                                ORDER BY count DESC
                                LIMIT 20
                            ) t
                        )
                    )
                ELSE NULL END AS facet_data
        ),
        total AS (
            SELECT count(*) as total_count FROM filtered_results
        )
        SELECT jsonb_build_object(
            'query', v_query,
            'total', total.total_count,
            'limit', v_limit,
            'offset', v_offset,
            'has_more', (v_offset + v_limit < total.total_count),
            'execution_time_ms', (EXTRACT(EPOCH FROM clock_timestamp() - start_time) * 1000)::INT,
            'results', COALESCE(jsonb_agg(rs.result_json), '[]'::jsonb),
            'facets', f.facet_data
        )
        FROM total, facets
        LEFT JOIN result_set rs ON true
        GROUP BY total.total_count, f.facet_data
    );

END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION dsa_search.search(INT, JSONB) IS
'Executes a full-featured search against datasets with dynamic filtering, sorting, pagination, and faceting, while enforcing user permissions. Accepts a user_id and a JSONB object of filter parameters.';


-- Suggestion Function: For fast autocomplete.
CREATE OR REPLACE FUNCTION dsa_search.suggest(
    p_user_id INT,
    p_query TEXT,
    p_limit INT DEFAULT 10
)
RETURNS JSONB AS $$
DECLARE
    start_time TIMESTAMPTZ;
BEGIN
    start_time := clock_timestamp();
    RETURN (
        WITH suggestions AS (
            (
                SELECT
                    ds.name AS text,
                    'dataset_name' AS type,
                    similarity(ds.name, p_query) AS score
                FROM dsa_search.datasets_summary ds
                JOIN dsa_auth.dataset_permissions dp ON ds.dataset_id = dp.dataset_id
                WHERE dp.user_id = p_user_id AND similarity(ds.name, p_query) > 0.1
                ORDER BY score DESC
                LIMIT p_limit
            )
            UNION ALL
            (
                SELECT
                    t.tag,
                    'tag' AS type,
                    max(similarity(t.tag, p_query)) as score
                FROM dsa_search.datasets_summary ds
                JOIN dsa_auth.dataset_permissions dp ON ds.dataset_id = dp.dataset_id,
                unnest(ds.tags) as t(tag)
                WHERE dp.user_id = p_user_id AND similarity(t.tag, p_query) > 0.2
                GROUP BY t.tag
                ORDER BY score DESC
                LIMIT p_limit
            )
            ORDER BY score DESC
            LIMIT p_limit
        )
        SELECT jsonb_build_object(
            'query', p_query,
            'execution_time_ms', (EXTRACT(EPOCH FROM clock_timestamp() - start_time) * 1000)::INT,
            'suggestions', COALESCE(jsonb_agg(
                jsonb_build_object(
                    'text', s.text,
                    'type', s.type,
                    'score', s.score
                ) ORDER BY s.score DESC
            ), '[]'::jsonb)
        )
        FROM suggestions s
    );
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION dsa_search.suggest(INT, TEXT, INT) IS
'Provides autocomplete suggestions for dataset names and tags based on user permissions and a partial query.';


--=============================================================================
-- 6. INITIAL DATA SETUP
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
--    `SET search_path TO dsa_core, dsa_jobs, dsa_search, dsa_auth, public;`
--
-- 2. PERMISSIONS: This structure allows for granular role-based access control.
--    For example, a background worker role can be granted permissions only on
--    the `dsa_jobs` schema, enhancing security.
--    `GRANT USAGE ON SCHEMA dsa_jobs TO worker_role;`
--    `GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA dsa_jobs TO worker_role;`
--=============================================================================