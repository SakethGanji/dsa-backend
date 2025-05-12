-- Get a single dataset version by ID with sheets
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
    u.soeid AS uploaded_by_soeid,
    (
        SELECT 
            json_agg(json_build_object(
                'id', s.id,
                'name', s.name,
                'sheet_index', s.sheet_index,
                'description', s.description,
                'metadata', sm.metadata
            ))
        FROM 
            sheets s
        LEFT JOIN sheet_metadata sm ON s.id = sm.sheet_id
        WHERE 
            s.dataset_version_id = dv.id
        ORDER BY 
            s.sheet_index
    ) AS sheets
FROM 
    dataset_versions dv
JOIN files f ON dv.file_id = f.id
LEFT JOIN users u ON dv.uploaded_by = u.id
WHERE 
    dv.id = :version_id;