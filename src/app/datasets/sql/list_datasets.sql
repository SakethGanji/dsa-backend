-- List datasets with filtering, sorting, and pagination
WITH filtered_datasets AS (
    SELECT 
        d.id,
        d.name,
        d.description,
        d.created_by,
        d.created_at,
        d.updated_at,
        array_agg(DISTINCT t.id) FILTER (WHERE t.id IS NOT NULL) AS tag_ids,
        array_agg(DISTINCT t.name) FILTER (WHERE t.name IS NOT NULL) AS tag_names,
        dv.version_number AS current_version,
        f.file_type,
        f.file_size
    FROM 
        datasets d
    LEFT JOIN dataset_tags dt ON d.id = dt.dataset_id
    LEFT JOIN tags t ON dt.tag_id = t.id
    LEFT JOIN dataset_versions dv ON d.id = dv.dataset_id
    LEFT JOIN files f ON dv.file_id = f.id
    WHERE 
        (:name IS NULL OR d.name ILIKE '%' || :name || '%')
        AND (:description IS NULL OR d.description ILIKE '%' || :description || '%')
        AND (:created_by IS NULL OR d.created_by = :created_by)
        AND (:tag IS NULL OR 
             CASE 
                 WHEN array_length(:tag::text[], 1) > 0 THEN 
                     EXISTS (SELECT 1 FROM dataset_tags dt2 
                             JOIN tags t2 ON dt2.tag_id = t2.id 
                             WHERE dt2.dataset_id = d.id 
                             AND t2.name = ANY(:tag::text[]))
                 ELSE true
             END)
        AND (:file_type IS NULL OR f.file_type = :file_type)
        AND (:file_size_min IS NULL OR f.file_size >= :file_size_min)
        AND (:file_size_max IS NULL OR f.file_size <= :file_size_max)
        AND (:version_min IS NULL OR dv.version_number >= :version_min)
        AND (:version_max IS NULL OR dv.version_number <= :version_max)
        AND (:created_at_from IS NULL OR d.created_at >= :created_at_from)
        AND (:created_at_to IS NULL OR d.created_at <= :created_at_to)
        AND (:updated_at_from IS NULL OR d.updated_at >= :updated_at_from)
        AND (:updated_at_to IS NULL OR d.updated_at <= :updated_at_to)
    GROUP BY 
        d.id, d.name, d.description, d.created_by, d.created_at, d.updated_at, dv.version_number, f.file_type, f.file_size
)
SELECT 
    id,
    name,
    description,
    created_by,
    created_at,
    updated_at,
    tag_ids,
    tag_names,
    current_version,
    file_type,
    file_size
FROM 
    filtered_datasets
ORDER BY 
    CASE WHEN :sort_by = 'name' AND :sort_order = 'asc' THEN name END ASC,
    CASE WHEN :sort_by = 'name' AND :sort_order = 'desc' THEN name END DESC,
    CASE WHEN :sort_by = 'created_at' AND :sort_order = 'asc' THEN created_at END ASC,
    CASE WHEN :sort_by = 'created_at' AND :sort_order = 'desc' THEN created_at END DESC,
    CASE WHEN :sort_by = 'updated_at' AND :sort_order = 'asc' THEN updated_at END ASC,
    CASE WHEN :sort_by = 'updated_at' AND :sort_order = 'desc' THEN updated_at END DESC,
    CASE WHEN :sort_by = 'file_size' AND :sort_order = 'asc' THEN file_size END ASC,
    CASE WHEN :sort_by = 'file_size' AND :sort_order = 'desc' THEN file_size END DESC,
    CASE WHEN :sort_by = 'current_version' AND :sort_order = 'asc' THEN current_version END ASC,
    CASE WHEN :sort_by = 'current_version' AND :sort_order = 'desc' THEN current_version END DESC,
    -- Default sort if no valid sort_by is provided or it's null
    CASE WHEN :sort_by IS NULL OR :sort_by NOT IN ('name', 'created_at', 'updated_at', 'file_size', 'current_version') THEN updated_at END DESC
LIMIT :limit
OFFSET :offset;