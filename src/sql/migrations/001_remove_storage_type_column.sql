-- Migration: Remove storage_type column and use self-describing URIs
-- This migration converts existing file paths to full URIs based on their storage_type

BEGIN;

-- Step 1: Update existing file_path values to be full URIs
UPDATE files 
SET file_path = 
    CASE 
        WHEN storage_type = 'local' THEN 'file://' || file_path
        WHEN storage_type = 's3' THEN 's3://' || file_path
        ELSE file_path  -- Keep as-is if storage_type is something else
    END
WHERE file_path NOT LIKE 'file://%' 
  AND file_path NOT LIKE 's3://%'
  AND file_path NOT LIKE 'http://%'
  AND file_path NOT LIKE 'https://%';

-- Step 2: Make file_path NOT NULL if it isn't already
ALTER TABLE files ALTER COLUMN file_path SET NOT NULL;

-- Step 3: Drop the storage_type column
ALTER TABLE files DROP COLUMN storage_type;

-- Step 4: Add a check constraint to ensure file_path is a valid URI
ALTER TABLE files ADD CONSTRAINT file_path_is_uri 
CHECK (
    file_path LIKE 'file://%' OR 
    file_path LIKE 's3://%' OR 
    file_path LIKE 'gs://%' OR 
    file_path LIKE 'azure://%' OR
    file_path LIKE 'http://%' OR
    file_path LIKE 'https://%'
);

-- Step 5: Create an index on the URI scheme for efficient backend selection
CREATE INDEX idx_files_uri_scheme ON files ((substring(file_path from '^[^:]+:')));

COMMIT;