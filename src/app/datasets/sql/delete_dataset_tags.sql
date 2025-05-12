-- Delete all tags for a dataset
DELETE FROM dataset_tags
WHERE dataset_id = :dataset_id;