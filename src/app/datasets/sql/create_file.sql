INSERT INTO files (storage_type, file_type, mime_type, file_data, file_path, file_size)
VALUES (:storage_type, :file_type, :mime_type, :file_data, :file_path, :file_size)
RETURNING id;