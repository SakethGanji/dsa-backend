-- =============================================================================
--          DSA PLATFORM - CONSOLIDATED UPDATED SCHEMA
--          Generated: 2025-07-30
--
-- This is the consolidated schema combining the full dump with all manual changes.
-- It includes:
-- 1. Authentication schema (dsa_auth)
-- 2. Core data model with Git-like versioning (dsa_core)
-- 3. Jobs and analysis queue (dsa_jobs)
-- 4. Search functionality with updated similarity threshold (dsa_search)
-- 5. Event sourcing and audit schemas (dsa_events, dsa_audit)
-- 6. Staging schema for data import (dsa_staging)
--
-- IMPORTANT CHANGES:
-- - Search similarity threshold is 0.05 for better fuzzy matching
-- - Added table_analysis table for comprehensive table-level statistics
-- - Removed commit_statistics table (replaced by table_analysis)
-- - Fixed search function to handle empty queries after keyword extraction
-- - This schema represents the current production state
-- =============================================================================

-- Drop schemas in reverse dependency order if needed for clean rebuild
-- DROP SCHEMA IF EXISTS dsa_staging CASCADE;
-- DROP SCHEMA IF EXISTS dsa_search CASCADE;
-- DROP SCHEMA IF EXISTS dsa_jobs CASCADE;
-- DROP SCHEMA IF EXISTS dsa_events CASCADE;
-- DROP SCHEMA IF EXISTS dsa_audit CASCADE;
-- DROP SCHEMA IF EXISTS dsa_core CASCADE;
-- DROP SCHEMA IF EXISTS dsa_auth CASCADE;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS dsa_auth;
CREATE SCHEMA IF NOT EXISTS dsa_core;
CREATE SCHEMA IF NOT EXISTS dsa_jobs;
CREATE SCHEMA IF NOT EXISTS dsa_search;
CREATE SCHEMA IF NOT EXISTS dsa_events;
CREATE SCHEMA IF NOT EXISTS dsa_audit;
CREATE SCHEMA IF NOT EXISTS dsa_staging;

-- Schema comments
COMMENT ON SCHEMA dsa_staging IS 'Staging area for V3 import system - contains temporary tables for high-performance data loading';

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;

-- Extension comments
COMMENT ON EXTENSION btree_gin IS 'support for indexing common datatypes in GIN';
COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';
COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';
COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';

-- =============================================================================
-- 1. AUTHENTICATION SCHEMA (dsa_auth)
-- =============================================================================

-- Create custom types
CREATE TYPE dsa_auth.dataset_permission AS ENUM ('read', 'write', 'admin');

-- Roles table
CREATE TABLE dsa_auth.roles (
    id SERIAL PRIMARY KEY,
    role_name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT
);

-- Users table
CREATE TABLE dsa_auth.users (
    id SERIAL PRIMARY KEY,
    soeid VARCHAR(20) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role_id INT NOT NULL REFERENCES dsa_auth.roles(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Dataset permissions table (cross-schema reference, created after dsa_core.datasets)

-- =============================================================================
-- 2. CORE DATA MODEL SCHEMA (dsa_core)
-- =============================================================================

-- Datasets table
CREATE TABLE dsa_core.datasets (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_by INT REFERENCES dsa_auth.users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(name, created_by)
);

-- Content-addressable row storage
CREATE TABLE dsa_core.rows (
    row_hash CHAR(64) PRIMARY KEY,
    data JSONB NOT NULL
);
COMMENT ON TABLE dsa_core.rows IS 'Content-addressable store for all unique data rows (blobs).';

-- Commits table
CREATE TABLE dsa_core.commits (
    commit_id CHAR(64) PRIMARY KEY,
    dataset_id INT NOT NULL REFERENCES dsa_core.datasets(id) ON DELETE CASCADE,
    parent_commit_id CHAR(64) REFERENCES dsa_core.commits(commit_id),
    message TEXT,
    author_id INT REFERENCES dsa_auth.users(id) ON DELETE SET NULL,
    authored_at TIMESTAMPTZ,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE dsa_core.commits IS 'An immutable, point-in-time snapshot of a dataset.';
CREATE INDEX idx_commits_dataset_id ON dsa_core.commits(dataset_id);

-- Commit rows junction table
CREATE TABLE dsa_core.commit_rows (
    commit_id CHAR(64) NOT NULL REFERENCES dsa_core.commits(commit_id) ON DELETE CASCADE,
    logical_row_id TEXT NOT NULL,
    row_hash CHAR(64) NOT NULL REFERENCES dsa_core.rows(row_hash),
    PRIMARY KEY (commit_id, logical_row_id)
);
COMMENT ON TABLE dsa_core.commit_rows IS 'The manifest linking a commit to its constituent rows.';
CREATE INDEX idx_commit_rows_row_hash ON dsa_core.commit_rows(row_hash);

-- Refs table (branches/tags)
CREATE TABLE dsa_core.refs (
    id SERIAL PRIMARY KEY,
    dataset_id INT NOT NULL REFERENCES dsa_core.datasets(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    commit_id CHAR(64) REFERENCES dsa_core.commits(commit_id) ON DELETE CASCADE,
    UNIQUE (dataset_id, name)
);
COMMENT ON TABLE dsa_core.refs IS 'Named pointers (branches, tags) to specific commits.';

-- Commit schemas table
CREATE TABLE dsa_core.commit_schemas (
    id SERIAL PRIMARY KEY,
    commit_id CHAR(64) NOT NULL REFERENCES dsa_core.commits(commit_id) ON DELETE CASCADE UNIQUE,
    schema_definition JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE dsa_core.commit_schemas IS 'Stores the schema definition(s) for a specific commit.';

-- Tags table
CREATE TABLE dsa_core.tags (
    id SERIAL PRIMARY KEY,
    tag_name VARCHAR(100) UNIQUE NOT NULL
);

-- Dataset tags junction table
CREATE TABLE dsa_core.dataset_tags (
    dataset_id INT NOT NULL REFERENCES dsa_core.datasets(id) ON DELETE CASCADE,
    tag_id INT NOT NULL REFERENCES dsa_core.tags(id) ON DELETE CASCADE,
    PRIMARY KEY (dataset_id, tag_id)
);

-- Table analysis table
CREATE TABLE dsa_core.table_analysis (
    commit_id TEXT NOT NULL,
    table_key TEXT NOT NULL,
    analysis JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (commit_id, table_key),
    FOREIGN KEY (commit_id) REFERENCES dsa_core.commits(commit_id) ON DELETE CASCADE
);
COMMENT ON TABLE dsa_core.table_analysis IS 'Stores comprehensive table-level analysis including column types, null counts, sample values, and statistics.';
CREATE INDEX idx_table_analysis_commit_id ON dsa_core.table_analysis(commit_id);
CREATE INDEX idx_table_analysis_table_key ON dsa_core.table_analysis(table_key);

-- =============================================================================
-- 3. CROSS-SCHEMA TABLES
-- =============================================================================

-- Dataset permissions (references both dsa_auth.users and dsa_core.datasets)
CREATE TABLE dsa_auth.dataset_permissions (
    dataset_id INT NOT NULL REFERENCES dsa_core.datasets(id) ON DELETE CASCADE,
    user_id INT NOT NULL REFERENCES dsa_auth.users(id) ON DELETE CASCADE,
    permission_type dsa_auth.dataset_permission NOT NULL,
    PRIMARY KEY (dataset_id, user_id)
);
CREATE INDEX idx_dataset_permissions_user_id ON dsa_auth.dataset_permissions(user_id);
CREATE INDEX idx_dataset_permissions_dataset_id ON dsa_auth.dataset_permissions(dataset_id);

-- =============================================================================
-- 4. JOBS & ANALYSIS SCHEMA (dsa_jobs)
-- =============================================================================

-- Create custom types
CREATE TYPE dsa_jobs.analysis_run_type AS ENUM ('import', 'sampling', 'exploration', 'profiling');
CREATE TYPE dsa_jobs.analysis_run_status AS ENUM ('pending', 'running', 'completed', 'failed');

-- Analysis configurations table
CREATE TABLE dsa_jobs.analysis_configurations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    analysis_type VARCHAR(100) NOT NULL,
    parameters JSONB NOT NULL,
    created_by INT REFERENCES dsa_auth.users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(name, created_by)
);

-- Analysis runs table
CREATE TABLE dsa_jobs.analysis_runs (
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
CREATE INDEX idx_analysis_runs_pending_jobs ON dsa_jobs.analysis_runs(status, run_type) WHERE status = 'pending';
CREATE INDEX idx_analysis_runs_dataset_id ON dsa_jobs.analysis_runs(dataset_id);

-- =============================================================================
-- 5. EVENT SOURCING SCHEMA (dsa_events)
-- =============================================================================

-- Domain events table
CREATE TABLE dsa_events.domain_events (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    occurred_at TIMESTAMP NOT NULL,
    user_id INT,
    correlation_id UUID,
    version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Aggregate snapshots table
CREATE TABLE dsa_events.aggregate_snapshots (
    aggregate_id VARCHAR(255) NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL,
    version INT NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (aggregate_id, aggregate_type, version)
);

-- Event sourcing indexes
CREATE INDEX idx_aggregate ON dsa_events.domain_events(aggregate_id, aggregate_type);
CREATE INDEX idx_event_type ON dsa_events.domain_events(event_type);
CREATE INDEX idx_occurred_at ON dsa_events.domain_events(occurred_at);
CREATE INDEX idx_user ON dsa_events.domain_events(user_id);
CREATE INDEX idx_correlation ON dsa_events.domain_events(correlation_id);
CREATE INDEX idx_latest ON dsa_events.aggregate_snapshots(aggregate_id, aggregate_type, version DESC);

-- =============================================================================
-- 6. AUDIT LOG SCHEMA (dsa_audit)
-- =============================================================================

-- Audit logs table
CREATE TABLE dsa_audit.audit_logs (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    user_id INT,
    action VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    occurred_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Audit log indexes
CREATE INDEX idx_aggregate ON dsa_audit.audit_logs(aggregate_type, aggregate_id);
CREATE INDEX idx_event_id ON dsa_audit.audit_logs(event_id);
CREATE INDEX idx_occurred ON dsa_audit.audit_logs(occurred_at);
CREATE INDEX idx_user ON dsa_audit.audit_logs(user_id);

-- =============================================================================
-- 7. STAGING SCHEMA (dsa_staging)
-- =============================================================================

-- CSV raw staging table
CREATE UNLOGGED TABLE dsa_staging.csv_raw (
    line_num SERIAL PRIMARY KEY,
    line_data TEXT
);
COMMENT ON TABLE dsa_staging.csv_raw IS 'Raw CSV lines for ultra-fast COPY operations';

-- Import data staging table
CREATE UNLOGGED TABLE dsa_staging.import_data (
    row_num BIGSERIAL PRIMARY KEY,
    sheet_name TEXT NOT NULL,
    row_data JSONB NOT NULL
);
COMMENT ON TABLE dsa_staging.import_data IS 'Structured staging for all file types with JSONB row data';
CREATE INDEX idx_import_row_num ON dsa_staging.import_data(row_num);

-- Commit manifest staging table
CREATE UNLOGGED TABLE dsa_staging.commit_manifest (
    logical_row_id TEXT PRIMARY KEY,
    row_hash CHAR(64) NOT NULL,
    row_num BIGINT NOT NULL
);
COMMENT ON TABLE dsa_staging.commit_manifest IS 'Maps logical row IDs to content hashes for commit assembly';
CREATE INDEX idx_manifest_hash ON dsa_staging.commit_manifest(row_hash);

-- =============================================================================
-- 8. SEARCH SCHEMA (dsa_search)
-- =============================================================================

-- Create search result type
CREATE TYPE dsa_search.search_result AS (
    id INTEGER,
    name VARCHAR(255),
    description TEXT,
    created_by INTEGER,
    created_by_name VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    tags TEXT[],
    score REAL,
    user_permission dsa_auth.dataset_permission
);

-- Create materialized view for search
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
    COALESCE(dta.tags, ARRAY[]::VARCHAR[]) AS tags,
    -- Concatenated text for trigram (fuzzy) search
    (d.name::TEXT || ' '::TEXT || COALESCE(d.description, ''::TEXT) || ' '::TEXT || 
     array_to_string(COALESCE(dta.tags, ARRAY[]::VARCHAR[]), ' '::TEXT)) AS search_text,
    -- TSVector for optimized full-text search
    (to_tsvector('english'::regconfig, d.name::TEXT) ||
     to_tsvector('english'::regconfig, COALESCE(d.description, ''::TEXT)) ||
     to_tsvector('english'::regconfig, array_to_string(COALESCE(dta.tags, ARRAY[]::VARCHAR[]), ' '::TEXT))) AS search_tsv
FROM dsa_core.datasets d
LEFT JOIN dsa_auth.users u ON d.created_by = u.id
LEFT JOIN dataset_tags_agg dta ON d.id = dta.dataset_id
WITH NO DATA;

-- Create indexes for search performance
CREATE UNIQUE INDEX idx_datasets_summary_id ON dsa_search.datasets_summary(dataset_id);
CREATE INDEX idx_datasets_summary_search_text_trgm ON dsa_search.datasets_summary USING gin (search_text gin_trgm_ops);
CREATE INDEX idx_datasets_summary_search_tsv ON dsa_search.datasets_summary USING gin (search_tsv);
CREATE INDEX idx_datasets_summary_tags ON dsa_search.datasets_summary USING gin (tags);
CREATE INDEX idx_datasets_summary_name ON dsa_search.datasets_summary(name);
CREATE INDEX idx_datasets_summary_created_at ON dsa_search.datasets_summary(created_at DESC);
CREATE INDEX idx_datasets_summary_updated_at ON dsa_search.datasets_summary(updated_at DESC);
CREATE INDEX idx_datasets_summary_created_by_id ON dsa_search.datasets_summary(created_by_id);
CREATE INDEX idx_datasets_summary_created_by_name ON dsa_search.datasets_summary(created_by_name);

-- =============================================================================
-- SEARCH FUNCTION: With 0.05 similarity threshold and keyword parsing fix
-- =============================================================================

CREATE OR REPLACE FUNCTION dsa_search.search(
    p_current_user_id INTEGER,
    p_query TEXT DEFAULT NULL,
    p_fuzzy BOOLEAN DEFAULT TRUE,
    p_tags TEXT[] DEFAULT NULL,
    p_created_by INTEGER[] DEFAULT NULL,
    p_created_after TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_created_before TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_updated_after TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_updated_before TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_limit INTEGER DEFAULT 20,
    p_offset INTEGER DEFAULT 0,
    p_sort_by TEXT DEFAULT 'relevance',
    p_sort_order TEXT DEFAULT 'desc',
    p_include_facets BOOLEAN DEFAULT TRUE,
    p_facet_fields TEXT[] DEFAULT ARRAY['tags', 'created_by']
)
RETURNS JSONB
LANGUAGE plpgsql
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

    -- Build search condition (only if there's actual search text)
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
    p_current_user_id INTEGER,
    p_query TEXT,
    p_limit INTEGER DEFAULT 10
)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_results JSONB; 
    v_start_time TIMESTAMPTZ := clock_timestamp();
BEGIN
    p_limit := LEAST(GREATEST(p_limit, 1), 50);
    
    WITH suggestions AS (
        -- Dataset name suggestions with permission check
        (SELECT 
            s.name AS text, 
            'dataset_name' AS type, 
            similarity(s.name, p_query) AS score
         FROM dsa_search.datasets_summary s
         JOIN dsa_auth.dataset_permissions dp ON s.dataset_id = dp.dataset_id
         WHERE s.name % p_query AND dp.user_id = p_current_user_id
         ORDER BY score DESC, length(s.name) ASC 
         LIMIT p_limit)
        
        UNION ALL
        
        -- Tag suggestions (no permission check needed)
        (SELECT 
            t.tag_name AS text, 
            'tag' AS type, 
            similarity(t.tag_name, p_query) AS score
         FROM dsa_core.tags t
         WHERE t.tag_name % p_query
         ORDER BY score DESC, length(t.tag_name) ASC 
         LIMIT p_limit)
    )
    SELECT COALESCE(jsonb_agg(s ORDER BY s.score DESC, s.type, s.text), '[]'::jsonb) 
    FROM (
        SELECT * FROM suggestions 
        ORDER BY score DESC, type, text 
        LIMIT p_limit
    ) s 
    INTO v_results;
    
    RETURN jsonb_build_object(
        'suggestions', v_results, 
        'query', p_query, 
        'execution_time_ms', (EXTRACT(EPOCH FROM clock_timestamp() - v_start_time) * 1000)::int
    );
END;
$$;

-- =============================================================================
-- 9. LEGACY PUBLIC SCHEMA TABLE (if needed)
-- =============================================================================

-- This appears to be a duplicate/legacy table from the dump
CREATE TABLE IF NOT EXISTS public.roles (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL
);

-- =============================================================================
-- 10. INITIAL DATA SETUP
-- =============================================================================

-- Insert default roles
INSERT INTO dsa_auth.roles (role_name, description) VALUES
    ('admin', 'Full system administrator'),
    ('analyst', 'Data analyst with read/write permissions'),
    ('viewer', 'Read-only access')
ON CONFLICT (role_name) DO NOTHING;

-- =============================================================================
-- 11. PERMISSIONS
-- =============================================================================

-- Grant usage on schemas
GRANT USAGE ON SCHEMA dsa_auth TO PUBLIC;
GRANT USAGE ON SCHEMA dsa_core TO PUBLIC;
GRANT USAGE ON SCHEMA dsa_jobs TO PUBLIC;
GRANT USAGE ON SCHEMA dsa_search TO PUBLIC;
GRANT USAGE ON SCHEMA dsa_events TO PUBLIC;
GRANT USAGE ON SCHEMA dsa_audit TO PUBLIC;
GRANT USAGE ON SCHEMA dsa_staging TO PUBLIC;

-- Grant select on materialized view
GRANT SELECT ON dsa_search.datasets_summary TO PUBLIC;

-- Grant execute on functions
GRANT EXECUTE ON FUNCTION dsa_search.search TO PUBLIC;
GRANT EXECUTE ON FUNCTION dsa_search.suggest TO PUBLIC;

-- =============================================================================
-- 12. MAINTENANCE
-- =============================================================================

-- Initial refresh of the materialized view (only if data exists)
-- REFRESH MATERIALIZED VIEW dsa_search.datasets_summary;

-- =============================================================================
--                                USAGE NOTES
-- =============================================================================
-- 1. SIMILARITY THRESHOLD: The search function uses a similarity threshold of 0.05
--    (lowered from 0.2) for better fuzzy matching.
--
-- 2. MATERIALIZED VIEW REFRESH: The search index should be refreshed periodically:
--    REFRESH MATERIALIZED VIEW CONCURRENTLY dsa_search.datasets_summary;
--
-- 3. SEARCH FUNCTION: Fixed to handle empty queries after keyword extraction
--    (e.g., when query is only "user:username" with no other text)
--
-- 4. TABLE ANALYSIS: The commit_statistics table has been replaced by table_analysis
--    which provides better granularity and more comprehensive statistics
--
-- 5. STAGING TABLES: All staging tables are UNLOGGED for better import performance
--    but will be lost on crash (acceptable for temporary staging data)
--
-- 6. PERMISSIONS: All application queries must use fully-qualified table names
--    (e.g., SELECT * FROM dsa_auth.users;) or set the search_path appropriately
-- =============================================================================