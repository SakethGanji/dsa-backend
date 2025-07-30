-- =============================================================================
--          DSA PLATFORM v2 - COMPLETE SCHEMA WITH ALL UPDATES
--          Last Updated: 2025-07-08
--
-- This is the FINAL, COMPREHENSIVE schema for the entire DSA platform.
-- It includes:
-- 1. Authentication schema (dsa_auth)
-- 2. Core data model with Git-like versioning (dsa_core)
-- 3. Jobs and analysis queue (dsa_jobs)
-- 4. Search functionality with updated similarity threshold (dsa_search)
--
-- IMPORTANT CHANGES:
-- - Search similarity threshold updated from 0.2 to 0.05 for better fuzzy matching
-- - Added table_analysis table for comprehensive table-level statistics
-- - Removed commit_statistics table (replaced by table_analysis)
-- - This schema represents the current production state
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

-- Table for storing comprehensive table analysis results
CREATE TABLE IF NOT EXISTS dsa_core.table_analysis (
    commit_id TEXT NOT NULL,
    table_key TEXT NOT NULL,
    analysis JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (commit_id, table_key),
    FOREIGN KEY (commit_id) REFERENCES dsa_core.commits(commit_id) ON DELETE CASCADE
);
COMMENT ON TABLE dsa_core.table_analysis IS 'Stores comprehensive table-level analysis including column types, null counts, sample values, and statistics.';

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_table_analysis_commit_id ON dsa_core.table_analysis(commit_id);
CREATE INDEX IF NOT EXISTS idx_table_analysis_table_key ON dsa_core.table_analysis(table_key);


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
-- 5. SEARCH SCHEMA (`dsa_search`) - WITH UPDATED SEARCH FUNCTION
--=============================================================================

-- Drop existing search objects to ensure clean state
DROP MATERIALIZED VIEW IF EXISTS dsa_search.datasets_summary CASCADE;
DROP TYPE IF EXISTS dsa_search.search_result CASCADE;

-- Create the materialized view for search
CREATE MATERIALIZED VIEW dsa_search.datasets_summary AS
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
    COALESCE(dta.tags, ARRAY[]::character varying[]) AS tags,
    -- Concatenated text for trigram (fuzzy) search
    (d.name::text || ' '::text || COALESCE(d.description, ''::text) || ' '::text || 
     array_to_string(COALESCE(dta.tags, ARRAY[]::character varying[]), ' '::text)) AS search_text,
    -- TSVector for optimized full-text search
    (to_tsvector('english'::regconfig, d.name::text) ||
     to_tsvector('english'::regconfig, COALESCE(d.description, ''::text)) ||
     to_tsvector('english'::regconfig, array_to_string(COALESCE(dta.tags, ARRAY[]::character varying[]), ' '::text))) AS search_tsv
FROM dsa_core.datasets d
LEFT JOIN dsa_auth.users u ON d.created_by = u.id
LEFT JOIN dataset_tags_agg dta ON d.id = dta.dataset_id;

-- Create indexes for optimal search performance
CREATE UNIQUE INDEX idx_datasets_summary_dataset_id ON dsa_search.datasets_summary(dataset_id);
CREATE INDEX idx_datasets_summary_search_text_trgm ON dsa_search.datasets_summary USING gin (search_text gin_trgm_ops);
CREATE INDEX idx_datasets_summary_search_tsv ON dsa_search.datasets_summary USING gin (search_tsv);
CREATE INDEX idx_datasets_summary_tags ON dsa_search.datasets_summary USING gin (tags);
CREATE INDEX idx_datasets_summary_name ON dsa_search.datasets_summary(name);
CREATE INDEX idx_datasets_summary_created_at ON dsa_search.datasets_summary(created_at DESC);
CREATE INDEX idx_datasets_summary_updated_at ON dsa_search.datasets_summary(updated_at DESC);
CREATE INDEX idx_datasets_summary_created_by_id ON dsa_search.datasets_summary(created_by_id);
CREATE INDEX idx_datasets_summary_created_by_name ON dsa_search.datasets_summary(created_by_name);

-- =============================================================================
-- SEARCH FUNCTION: Updated with 0.05 similarity threshold
-- =============================================================================

CREATE OR REPLACE FUNCTION dsa_search.search(
    p_current_user_id integer, 
    p_query text DEFAULT NULL::text, 
    p_fuzzy boolean DEFAULT true, 
    p_tags text[] DEFAULT NULL::text[], 
    p_created_by integer[] DEFAULT NULL::integer[], 
    p_created_after timestamp with time zone DEFAULT NULL::timestamp with time zone, 
    p_created_before timestamp with time zone DEFAULT NULL::timestamp with time zone, 
    p_updated_after timestamp with time zone DEFAULT NULL::timestamp with time zone, 
    p_updated_before timestamp with time zone DEFAULT NULL::timestamp with time zone, 
    p_limit integer DEFAULT 20, 
    p_offset integer DEFAULT 0, 
    p_sort_by text DEFAULT 'relevance'::text, 
    p_sort_order text DEFAULT 'desc'::text, 
    p_include_facets boolean DEFAULT true, 
    p_facet_fields text[] DEFAULT ARRAY['tags'::text, 'created_by'::text]
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_base_from TEXT; 
    v_where_clauses TEXT[] := '{}'; 
    v_order_by_clause TEXT;
    v_sql TEXT; 
    v_results JSONB; 
    v_facets JSONB := '{}'::jsonb; 
    v_total_count BIGINT;
    v_start_time TIMESTAMPTZ := clock_timestamp(); 
    v_main_query_text TEXT := p_query;
    v_kv_match RECORD; 
    v_parsed_tags TEXT[] := '{}'; 
    v_parsed_users TEXT[] := '{}';
BEGIN
    -- Validate and sanitize inputs
    p_limit := LEAST(GREATEST(p_limit, 1), 100); 
    p_offset := GREATEST(p_offset, 0);

    -- PERMISSION MODEL: Uses an INNER JOIN for performance
    v_base_from := format(
        'FROM dsa_search.datasets_summary s 
         JOIN dsa_auth.dataset_permissions dp ON s.dataset_id = dp.dataset_id AND dp.user_id = %L',
        p_current_user_id
    );

    -- Parse special keywords from query (tag:value, user:value, by:value)
    IF v_main_query_text IS NOT NULL AND v_main_query_text <> '' THEN
        FOR v_kv_match IN 
            SELECT (regexp_matches(v_main_query_text, '(tag|user|by):(\w+)', 'g')) AS m 
        LOOP
            DECLARE 
                v_key TEXT := v_kv_match.m[1]; 
                v_value TEXT := v_kv_match.m[2];
            BEGIN
                CASE v_key 
                    WHEN 'tag' THEN v_parsed_tags := v_parsed_tags || v_value; 
                    WHEN 'user', 'by' THEN v_parsed_users := v_parsed_users || v_value; 
                END CASE;
            END;
        END LOOP;
        
        -- Remove parsed keywords from main query
        v_main_query_text := regexp_replace(v_main_query_text, '(tag|user|by):(\w+)', '', 'g');
        v_main_query_text := trim(regexp_replace(v_main_query_text, '\s{2,}', ' ', 'g'));
        
        -- IMPORTANT FIX: Set to NULL if empty after keyword extraction
        IF v_main_query_text = '' THEN
            v_main_query_text := NULL;
        END IF;
    END IF;

    -- Build search condition
    IF v_main_query_text IS NOT NULL AND v_main_query_text <> '' THEN
        IF p_fuzzy THEN 
            v_where_clauses := v_where_clauses || format('similarity(s.search_text, %L) > 0.05', v_main_query_text);
        ELSE 
            -- Additional safety check for tsquery
            DECLARE
                v_tsquery_text TEXT := array_to_string(regexp_split_to_array(v_main_query_text, E'\\s+'), ' & ');
            BEGIN
                -- Only add tsquery condition if the text is valid
                IF v_tsquery_text <> '' THEN
                    v_where_clauses := v_where_clauses || format(
                        's.search_tsv @@ to_tsquery(''english'', %L)',
                        v_tsquery_text
                    );
                END IF;
            END; 
        END IF;
    END IF;
    
    -- Add filter conditions
    DECLARE 
        v_all_tags TEXT[] := COALESCE(p_tags, '{}') || v_parsed_tags;
    BEGIN
        IF array_length(v_all_tags, 1) > 0 THEN 
            v_where_clauses := v_where_clauses || format('s.tags @> %L', v_all_tags); 
        END IF;
        IF p_created_by IS NOT NULL THEN 
            v_where_clauses := v_where_clauses || format('s.created_by_id = ANY(%L)', p_created_by); 
        END IF;
        IF array_length(v_parsed_users, 1) > 0 THEN 
            v_where_clauses := v_where_clauses || format('s.created_by_name = ANY(%L)', v_parsed_users); 
        END IF;
    END;
    
    -- Date filters
    IF p_created_after IS NOT NULL THEN 
        v_where_clauses := v_where_clauses || format('s.created_at >= %L', p_created_after); 
    END IF; 
    IF p_created_before IS NOT NULL THEN 
        v_where_clauses := v_where_clauses || format('s.created_at <= %L', p_created_before); 
    END IF; 
    IF p_updated_after IS NOT NULL THEN 
        v_where_clauses := v_where_clauses || format('s.updated_at >= %L', p_updated_after); 
    END IF; 
    IF p_updated_before IS NOT NULL THEN 
        v_where_clauses := v_where_clauses || format('s.updated_at <= %L', p_updated_before); 
    END IF;

    -- Build ORDER BY clause
    v_order_by_clause := CASE
        WHEN v_main_query_text IS NOT NULL AND v_main_query_text <> '' AND p_sort_by = 'relevance' THEN 
            format('similarity(fr.search_text, %L) %s', v_main_query_text, p_sort_order)
        WHEN p_sort_by = 'name' THEN 'fr.name ' || p_sort_order 
        WHEN p_sort_by = 'created_at' THEN 'fr.created_at ' || p_sort_order 
        WHEN p_sort_by = 'updated_at' THEN 'fr.updated_at ' || p_sort_order
        ELSE 'fr.updated_at DESC' 
    END;

    -- Build and execute main query
    DECLARE
        v_where_string TEXT := CASE 
            WHEN array_length(v_where_clauses, 1) > 0 THEN 
                'WHERE ' || array_to_string(v_where_clauses, ' AND ') 
            ELSE '' 
        END;
        v_base_cte TEXT := format(
            'WITH filtered_results AS (
                SELECT s.*, dp.permission_type as user_permission %s %s
            )', 
            v_base_from, v_where_string
        );
    BEGIN
        -- Get results and total count
        v_sql := v_base_cte || format(' 
            SELECT 
                (SELECT COUNT(*) FROM filtered_results), 
                (SELECT COALESCE(jsonb_agg(r), ''[]''::jsonb) 
                 FROM (
                     SELECT 
                         fr.dataset_id AS id, 
                         fr.name, 
                         fr.description, 
                         fr.created_by_id as created_by, 
                         fr.created_by_name, 
                         fr.created_at, 
                         fr.updated_at, 
                         fr.tags, 
                         (CASE WHEN %L IS NOT NULL THEN similarity(fr.search_text, %L) ELSE NULL END)::real AS score, 
                         fr.user_permission 
                     FROM filtered_results fr 
                     ORDER BY %s 
                     LIMIT %L 
                     OFFSET %L
                 ) r)', 
            v_main_query_text, v_main_query_text, v_order_by_clause, p_limit, p_offset
        );
        
        EXECUTE v_sql INTO v_total_count, v_results;
        
        -- Calculate facets if requested
        IF p_include_facets THEN
            DECLARE v_tag_facets jsonb; v_created_by_facets jsonb;
            BEGIN
                IF 'tags' = ANY(p_facet_fields) THEN 
                    EXECUTE v_base_cte || ' 
                        SELECT COALESCE(jsonb_object_agg(value, count), ''{}''::jsonb)
                        FROM (
                            SELECT unnest(tags) as value, COUNT(*) as count 
                            FROM filtered_results 
                            GROUP BY value 
                            ORDER BY count DESC 
                            LIMIT 50
                        ) t;' 
                    INTO v_tag_facets;
                    v_facets := jsonb_set(v_facets, '{tags}', v_tag_facets);
                END IF;
                
                IF 'created_by' = ANY(p_facet_fields) THEN 
                    EXECUTE v_base_cte || ' 
                        SELECT COALESCE(jsonb_object_agg(value, count), ''{}''::jsonb)
                        FROM (
                            SELECT created_by_name as value, COUNT(*) as count 
                            FROM filtered_results 
                            WHERE created_by_name IS NOT NULL 
                            GROUP BY value 
                            ORDER BY count DESC 
                            LIMIT 50
                        ) t;' 
                    INTO v_created_by_facets;
                    v_facets := jsonb_set(v_facets, '{created_by}', v_created_by_facets);
                END IF;
            END;
        END IF;
    END;

    -- Return results
    RETURN jsonb_build_object(
        'results', v_results, 
        'total', v_total_count, 
        'limit', p_limit, 
        'offset', p_offset, 
        'has_more', (p_offset + p_limit) < v_total_count, 
        'query', p_query, 
        'execution_time_ms', (EXTRACT(EPOCH FROM clock_timestamp() - v_start_time) * 1000)::int, 
        'facets', CASE WHEN p_include_facets THEN v_facets ELSE NULL END
    );
END;
$$;

-- =============================================================================
-- SUGGEST FUNCTION: For autocomplete functionality
-- =============================================================================

CREATE OR REPLACE FUNCTION dsa_search.suggest(
    p_current_user_id integer, 
    p_query text, 
    p_limit integer DEFAULT 10
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_results JSONB;
    v_start_time TIMESTAMPTZ := clock_timestamp();
BEGIN
    -- Validate limit
    p_limit := LEAST(GREATEST(p_limit, 1), 50);

    WITH user_datasets AS (
        SELECT dataset_id
        FROM dsa_auth.dataset_permissions
        WHERE user_id = p_current_user_id
    ),
    dataset_suggestions AS (
        SELECT
            s.name AS suggestion,
            'dataset' AS type,
            similarity(s.name, p_query) AS score
        FROM dsa_search.datasets_summary s
        WHERE s.dataset_id IN (SELECT dataset_id FROM user_datasets)
          AND s.name % p_query  -- Using trigram similarity operator
        ORDER BY score DESC, length(s.name)
        LIMIT p_limit
    ),
    tag_suggestions AS (
        SELECT DISTINCT
            unnest(s.tags) AS suggestion,
            'tag' AS type,
            similarity(unnest(s.tags), p_query) AS score
        FROM dsa_search.datasets_summary s
        WHERE s.dataset_id IN (SELECT dataset_id FROM user_datasets)
          AND EXISTS (
              SELECT 1 FROM unnest(s.tags) AS t
              WHERE t % p_query
          )
        ORDER BY score DESC, length(suggestion)
        LIMIT p_limit
    ),
    all_suggestions AS (
        SELECT * FROM dataset_suggestions
        UNION ALL
        SELECT * FROM tag_suggestions
    )
    SELECT jsonb_agg(
        jsonb_build_object(
            'text', suggestion,
            'type', type,
            'score', score
        ) ORDER BY score DESC, type, suggestion
    )
    FROM (
        SELECT * FROM all_suggestions
        ORDER BY score DESC
        LIMIT p_limit
    ) s
    INTO v_results;

    RETURN jsonb_build_object(
        'suggestions', COALESCE(v_results, '[]'::jsonb),
        'query', p_query,
        'limit', p_limit,
        'execution_time_ms', EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start_time)::int
    );
END;
$$;

-- =============================================================================
-- 6. INITIAL DATA SETUP
-- =============================================================================

-- Insert default roles
INSERT INTO dsa_auth.roles (role_name, description) VALUES
    ('admin', 'Full system administrator'),
    ('analyst', 'Data analyst with read/write permissions'),
    ('viewer', 'Read-only access')
ON CONFLICT (role_name) DO NOTHING;

-- =============================================================================
-- 7. PERMISSIONS
-- =============================================================================

-- Grant usage on schemas
GRANT USAGE ON SCHEMA dsa_search TO PUBLIC;
GRANT USAGE ON SCHEMA dsa_core TO PUBLIC;
GRANT USAGE ON SCHEMA dsa_auth TO PUBLIC;
GRANT USAGE ON SCHEMA dsa_jobs TO PUBLIC;

-- Grant select on materialized view
GRANT SELECT ON dsa_search.datasets_summary TO PUBLIC;

-- Grant execute on functions
GRANT EXECUTE ON FUNCTION dsa_search.search TO PUBLIC;
GRANT EXECUTE ON FUNCTION dsa_search.suggest TO PUBLIC;

-- =============================================================================
-- 8. MAINTENANCE
-- =============================================================================

-- Initial refresh of the materialized view
REFRESH MATERIALIZED VIEW dsa_search.datasets_summary;

-- =============================================================================
--                                USAGE NOTES
-- =============================================================================
-- 1. SIMILARITY THRESHOLD: The search function uses a similarity threshold of 0.05
--    (lowered from 0.2) for better fuzzy matching. This is found at line 304.
--
-- 2. MATERIALIZED VIEW REFRESH: The search index should be refreshed periodically:
--    REFRESH MATERIALIZED VIEW CONCURRENTLY dsa_search.datasets_summary;
--
-- 3. SEARCH USAGE: The search function accepts multiple parameters including:
--    - Fuzzy text search with trigram similarity
--    - Tag filtering
--    - Date range filtering
--    - User filtering
--    - Pagination and sorting
--
-- 4. PERMISSIONS: All application queries must use fully-qualified table names
--    (e.g., `SELECT * FROM dsa_auth.users;`). Alternatively, set the search_path:
--    `SET search_path TO dsa_core, dsa_jobs, dsa_search, dsa_auth, public;`
-- =============================================================================
