INSERT INTO datasets (name, description, created_by) 
VALUES (:name, :description, :created_by)
ON CONFLICT (id) DO UPDATE 
SET name = :name,
    description = :description,
    updated_at = NOW()
RETURNING id;