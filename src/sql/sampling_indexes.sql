-- Sampling Performance Indexes for DSA
-- These indexes are critical for efficient SQL-based sampling operations

-- Essential indexes for sampling performance
CREATE INDEX IF NOT EXISTS idx_commit_rows_commit_logical 
ON dsa_core.commit_rows(commit_id, logical_row_id);
-- The logical_row_id index supports LIKE patterns with fixed prefixes

-- Index for hash-based filtering in deterministic sampling
CREATE INDEX IF NOT EXISTS idx_commit_rows_row_hash 
ON dsa_core.commit_rows(row_hash);

-- Partial index for faster table-specific queries
-- This helps when filtering by logical_row_id patterns
CREATE INDEX IF NOT EXISTS idx_commit_rows_logical_prefix 
ON dsa_core.commit_rows(commit_id, left(logical_row_id, 20));

-- CRITICAL for stratified/cluster sampling performance
-- Without these, JSONB extraction forces full table scans
-- Add indexes for commonly used stratification columns
-- Example indexes (adjust based on your actual column names):

-- CREATE INDEX idx_rows_data_region ON dsa_core.rows ((data->>'region'));
-- CREATE INDEX idx_rows_data_category ON dsa_core.rows ((data->>'product_category'));
-- CREATE INDEX idx_rows_data_department ON dsa_core.rows ((data->>'department_id'));
-- CREATE INDEX idx_rows_data_customer_segment ON dsa_core.rows ((data->>'customer_segment'));
-- CREATE INDEX idx_rows_data_date ON dsa_core.rows ((data->>'date'));

-- GIN index for flexible JSONB queries (optional, but helpful for ad-hoc queries)
CREATE INDEX IF NOT EXISTS idx_rows_data_gin 
ON dsa_core.rows USING GIN (data);

-- Index for faster row counting
CREATE INDEX IF NOT EXISTS idx_commit_rows_commit_count 
ON dsa_core.commit_rows(commit_id);

-- Analyze tables to update statistics for query planner
ANALYZE dsa_core.commit_rows;
ANALYZE dsa_core.rows;

-- Function to create indexes for specific columns dynamically
CREATE OR REPLACE FUNCTION create_sampling_column_index(column_name text) 
RETURNS void AS $$
BEGIN
    -- Validate column name to prevent SQL injection
    IF column_name !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RAISE EXCEPTION 'Invalid column name: %', column_name;
    END IF;
    
    -- Create the index
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_rows_data_%s ON dsa_core.rows ((data->>%L))',
                   column_name, column_name);
    
    -- Log the creation
    RAISE NOTICE 'Created index for column: %', column_name;
END;
$$ LANGUAGE plpgsql;

-- Example usage:
-- SELECT create_sampling_column_index('region');
-- SELECT create_sampling_column_index('product_category');

-- View to check sampling index coverage
CREATE OR REPLACE VIEW sampling_index_coverage AS
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef,
    pg_size_pretty(pg_relation_size(indexname::regclass)) as index_size
FROM pg_indexes
WHERE schemaname = 'dsa_core'
AND tablename IN ('commit_rows', 'rows')
ORDER BY tablename, indexname;

-- Query to identify missing indexes for frequently used columns
-- Run this periodically to identify columns that need indexes
CREATE OR REPLACE VIEW sampling_column_usage AS
WITH column_stats AS (
    SELECT 
        jsonb_object_keys(data) as column_name,
        COUNT(*) as usage_count
    FROM dsa_core.rows
    TABLESAMPLE SYSTEM(1) -- Sample 1% for performance
    GROUP BY 1
    ORDER BY 2 DESC
)
SELECT 
    cs.column_name,
    cs.usage_count,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM pg_indexes 
            WHERE schemaname = 'dsa_core' 
            AND tablename = 'rows' 
            AND indexdef LIKE '%' || cs.column_name || '%'
        ) THEN 'Indexed'
        ELSE 'Not Indexed'
    END as index_status
FROM column_stats cs
WHERE cs.usage_count > 100; -- Only show frequently used columns

-- Performance monitoring view for sampling queries
CREATE OR REPLACE VIEW sampling_query_performance AS
SELECT 
    query,
    calls,
    total_exec_time,
    mean_exec_time,
    stddev_exec_time,
    rows
FROM pg_stat_statements
WHERE query LIKE '%dsa_core.commit_rows%'
AND query LIKE '%TABLESAMPLE%' OR query LIKE '%LIMIT%'
ORDER BY mean_exec_time DESC
LIMIT 20;

-- Note: pg_stat_statements extension must be enabled for the above view
-- CREATE EXTENSION IF NOT EXISTS pg_stat_statements;