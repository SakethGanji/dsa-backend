SELECT
  u.id,
  u.soeid,
  u.role_id,
  r.role_name,
  u.created_at,
  u.updated_at
FROM users u
JOIN roles r ON u.role_id = r.id;