INSERT INTO tags (name, description)
VALUES (:name, :description)
ON CONFLICT (name) DO UPDATE 
SET description = :description
RETURNING id;