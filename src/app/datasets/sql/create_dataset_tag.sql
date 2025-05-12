INSERT INTO dataset_tags (dataset_id, tag_id)
VALUES (:dataset_id, :tag_id)
ON CONFLICT DO NOTHING;