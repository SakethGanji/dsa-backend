-- Simplified list datasets with basic filtering
WITH dataset_data AS (
    SELECT 
        d.id,
        d.name,
        d.description,
        d.created_by,
        d.created_at,
        d.updated_at,
        array_agg(DISTINCT t.id) FILTER (WHERE t.id IS NOT NULL) AS tag_ids,
        array_agg(DISTINCT t.name) FILTER (WHERE t.name IS NOT NULL) AS tag_names
    FROM 
        datasets d
    LEFT JOIN dataset_tags dt ON d.id = dt.dataset_id
    LEFT JOIN tags t ON dt.tag_id = t.id
    WHERE 
        (:name IS NULL OR d.name ILIKE '%' || :name || '%')
        AND (:created_by IS NULL OR d.created_by = :created_by)
    GROUP BY 
        d.id, d.name, d.description, d.created_by, d.created_at, d.updated_at
)
SELECT 
    dd.id,
    dd.name,
    dd.description,
    dd.created_by,
    dd.created_at,
    dd.updated_at,
    dd.tag_ids,
    dd.tag_names,
    dv.version_number as current_version,
    f.file_type,
    f.file_size
FROM 
    dataset_data dd
LEFT JOIN LATERAL (
    SELECT version_number, file_id
    FROM dataset_versions
    WHERE dataset_id = dd.id
    ORDER BY version_number DESC
    LIMIT 1
) dv ON true
LEFT JOIN files f ON dv.file_id = f.id
ORDER BY 
    dd.updated_at DESC
LIMIT :limit
OFFSET :offset;