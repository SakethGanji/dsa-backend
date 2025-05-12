-- List all tags with their usage count
SELECT 
    t.id,
    t.name,
    t.description,
    COUNT(dt.dataset_id) AS usage_count
FROM 
    tags t
LEFT JOIN dataset_tags dt ON t.id = dt.tag_id
GROUP BY 
    t.id, t.name, t.description
ORDER BY 
    t.name;