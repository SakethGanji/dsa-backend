-- Get a single dataset by ID with its tags and versions
WITH dataset_data AS (
    SELECT 
        d.id,
        d.name,
        d.description,
        d.created_by,
        d.created_at,
        d.updated_at,
        array_agg(DISTINCT t.id) FILTER (WHERE t.id IS NOT NULL) AS tag_ids,
        array_agg(DISTINCT t.name) FILTER (WHERE t.name IS NOT NULL) AS tag_names,
        array_agg(DISTINCT json_build_object(
            'id', t.id,
            'name', t.name,
            'description', t.description
        )) FILTER (WHERE t.id IS NOT NULL) AS tags
    FROM 
        datasets d
    LEFT JOIN dataset_tags dt ON d.id = dt.dataset_id
    LEFT JOIN tags t ON dt.tag_id = t.id
    WHERE 
        d.id = :dataset_id
    GROUP BY 
        d.id, d.name, d.description, d.created_by, d.created_at, d.updated_at
),
versions_data AS (
    SELECT 
        dv.dataset_id,
        array_agg(json_build_object(
            'id', dv.id,
            'dataset_id', dv.dataset_id,
            'version_number', dv.version_number,
            'file_id', dv.file_id,
            'uploaded_by', dv.uploaded_by,
            'ingestion_timestamp', dv.ingestion_timestamp,
            'last_updated_timestamp', dv.last_updated_timestamp
        ) ORDER BY dv.version_number DESC) AS versions,
        json_build_object(
            'id', f.id,
            'storage_type', f.storage_type,
            'file_type', f.file_type,
            'mime_type', f.mime_type,
            'file_size', f.file_size,
            'created_at', f.created_at
        ) AS file_info
    FROM 
        dataset_versions dv
    JOIN files f ON dv.file_id = f.id
    WHERE 
        dv.dataset_id = :dataset_id
    GROUP BY 
        dv.dataset_id, f.id, f.storage_type, f.file_type, f.mime_type, f.file_size, f.created_at
)
SELECT 
    dd.*,
    vd.versions,
    vd.file_info
FROM 
    dataset_data dd
LEFT JOIN versions_data vd ON dd.id = vd.dataset_id
WHERE 
    dd.id = :dataset_id;