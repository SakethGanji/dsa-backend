-- Get sheets for a dataset version
SELECT 
    s.id,
    s.name,
    s.sheet_index,
    s.description,
    sm.metadata
FROM 
    sheets s
LEFT JOIN sheet_metadata sm ON s.id = sm.sheet_id
WHERE 
    s.dataset_version_id = :version_id
ORDER BY 
    s.sheet_index;