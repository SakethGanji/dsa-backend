-- Migration: Drop deprecated commit_statistics table
-- Date: 2025-07-08
-- 
-- The commit_statistics table has been replaced by table_analysis
-- which provides better table-level granularity and more comprehensive data

-- Drop the deprecated table
DROP TABLE IF EXISTS dsa_core.commit_statistics CASCADE;

-- Verify it's gone
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_schema = 'dsa_core' 
        AND table_name = 'commit_statistics'
    ) THEN
        RAISE EXCEPTION 'commit_statistics table still exists after DROP';
    ELSE
        RAISE NOTICE 'commit_statistics table successfully removed';
    END IF;
END $$;