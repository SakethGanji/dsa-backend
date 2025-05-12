INSERT INTO sheet_metadata (sheet_id, metadata, profiling_report_file_id)
VALUES (:sheet_id, :metadata, :profiling_report_file_id)
RETURNING id;