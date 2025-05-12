INSERT INTO users (soeid, password_hash, role_id)
VALUES (:soeid, :password_hash, :role_id)
RETURNING *;
