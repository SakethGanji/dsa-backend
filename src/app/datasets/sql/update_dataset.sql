-- Update dataset metadata
UPDATE datasets
SET 
    name = COALESCE(:name, name),
    description = COALESCE(:description, description),
    updated_at = NOW()
WHERE 
    id = :dataset_id
RETURNING id;