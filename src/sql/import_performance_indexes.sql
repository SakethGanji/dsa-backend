-- =============================================================================
-- Import Performance Optimization Indexes
-- 
-- These indexes improve the performance of the import process, particularly
-- for duplicate detection and commit_rows operations.
-- =============================================================================

-- Add index on rows.row_hash for faster duplicate checking during imports
CREATE INDEX IF NOT EXISTS idx_rows_row_hash 
ON dsa_core.rows(row_hash);

-- Add index on rows.created_at for time-based queries
CREATE INDEX IF NOT EXISTS idx_rows_created_at 
ON dsa_core.rows(created_at);

-- Add composite index on commit_rows for faster lookups
CREATE INDEX IF NOT EXISTS idx_commit_rows_commit_hash 
ON dsa_core.commit_rows(commit_id, row_hash);

-- Add index for finding rows by logical_row_id within a commit
CREATE INDEX IF NOT EXISTS idx_commit_rows_logical_id
ON dsa_core.commit_rows(commit_id, logical_row_id);

-- Add progress and checkpoint columns to analysis_runs table
ALTER TABLE dsa_jobs.analysis_runs 
    ADD COLUMN IF NOT EXISTS progress JSONB,
    ADD COLUMN IF NOT EXISTS checkpoint JSONB;

-- Create index for finding resumable jobs
CREATE INDEX IF NOT EXISTS idx_analysis_runs_checkpoint 
    ON dsa_jobs.analysis_runs(id) 
    WHERE checkpoint IS NOT NULL;

-- Index for finding pending import jobs efficiently
CREATE INDEX IF NOT EXISTS idx_analysis_runs_pending_imports
    ON dsa_jobs.analysis_runs(created_at)
    WHERE status = 'pending'::dsa_jobs.analysis_run_status 
      AND run_type = 'import'::dsa_jobs.analysis_run_type;

-- Analyze tables to update statistics for query planner
ANALYZE dsa_core.rows;
ANALYZE dsa_core.commit_rows;
ANALYZE dsa_jobs.analysis_runs;