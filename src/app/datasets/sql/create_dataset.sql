INSERT INTO datasets (name, description, created_by) 
VALUES (:name, :description, :created_by)
RETURNING id;