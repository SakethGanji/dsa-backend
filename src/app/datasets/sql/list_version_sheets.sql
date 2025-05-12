-- Get all sheets for a dataset version
SELECT 
    id,
    name, 
    sheet_index,
    description
FROM 
    sheets
WHERE 
    dataset_version_id = :version_id
ORDER BY 
    sheet_index;