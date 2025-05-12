-- Get versions for a dataset
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
    dv.dataset_id = :dataset_id
ORDER BY 
    dv.version_number DESC;