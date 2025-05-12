UPDATE datasets 
SET updated_at = NOW() 
WHERE id = :dataset_id;