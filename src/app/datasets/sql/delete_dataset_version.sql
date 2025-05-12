-- Delete a dataset version
-- This doesn't delete the associated file or sheets to allow for potential recovery
-- A background job could clean up orphaned files later
DELETE FROM dataset_versions
WHERE id = :version_id
RETURNING id;