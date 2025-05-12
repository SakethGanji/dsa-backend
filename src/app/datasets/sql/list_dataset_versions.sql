-- List all versions for a specific dataset
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
    f.file_size,
    u.soeid AS uploaded_by_soeid
FROM 
    dataset_versions dv
JOIN files f ON dv.file_id = f.id
LEFT JOIN users u ON dv.uploaded_by = u.id
WHERE 
    dv.dataset_id = :dataset_id
ORDER BY 
    dv.version_number DESC;