-- Get a single dataset by ID with its tags and versions (simplified)
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
    d.id = :dataset_id
GROUP BY 
    d.id, d.name, d.description, d.created_by, d.created_at, d.updated_at;