WITH inserted AS (
  INSERT INTO users (soeid, password_hash, role_id)
  VALUES (:soeid, :password_hash, :role_id)
  RETURNING *
)
SELECT 
  i.*,
  r.role_name
FROM inserted i
JOIN roles r ON i.role_id = r.id;
