-- Get a single dataset version by ID (simplified)
SELECT 
    dv.id,
    dv.dataset_id,
    dv.version_number,
    dv.file_id,
    dv.uploaded_by,
    dv.ingestion_timestamp,
    dv.last_updated_timestamp,
    f.storage_type,
    f.file_type,
    f.mime_type,
    f.file_size
FROM 
    dataset_versions dv
JOIN files f ON dv.file_id = f.id
WHERE 
    dv.id = :version_id;