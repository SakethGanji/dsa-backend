INSERT INTO sheets (dataset_version_id, name, sheet_index, description)
VALUES (:dataset_version_id, :name, :sheet_index, :description)
RETURNING id;