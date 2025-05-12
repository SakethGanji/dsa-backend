INSERT INTO dataset_versions (dataset_id, version_number, file_id, uploaded_by)
VALUES (:dataset_id, :version_number, :file_id, :uploaded_by)
RETURNING id;