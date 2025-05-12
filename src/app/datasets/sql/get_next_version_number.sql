SELECT COALESCE(MAX(version_number), 0) + 1 as next_version
FROM dataset_versions
WHERE dataset_id = :dataset_id;