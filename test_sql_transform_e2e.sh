#!/bin/bash

# End-to-end test for sql-transform endpoint

echo "=== SQL Transform Endpoint Test ==="

# First, we need to set up test data
echo "1. Setting up test database..."

# Create test tables and data
PGPASSWORD=postgres psql -h localhost -U postgres -d postgres << 'EOF'
-- Create test schema if not exists
CREATE SCHEMA IF NOT EXISTS dsa_core;
CREATE SCHEMA IF NOT EXISTS dsa_auth;
CREATE SCHEMA IF NOT EXISTS dsa_jobs;

-- Create minimal tables for testing
CREATE TABLE IF NOT EXISTS dsa_auth.users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS dsa_core.datasets (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_by INT REFERENCES dsa_auth.users(id),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dsa_core.commits (
    commit_id CHAR(64) PRIMARY KEY,
    dataset_id INT NOT NULL REFERENCES dsa_core.datasets(id),
    parent_commit_id CHAR(64),
    message TEXT,
    author_id INT REFERENCES dsa_auth.users(id),
    committed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dsa_core.rows (
    row_hash CHAR(64) PRIMARY KEY,
    data JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS dsa_core.commit_rows (
    commit_id CHAR(64) NOT NULL REFERENCES dsa_core.commits(commit_id),
    logical_row_id TEXT NOT NULL,
    row_hash CHAR(64) NOT NULL REFERENCES dsa_core.rows(row_hash),
    PRIMARY KEY (commit_id, logical_row_id)
);

CREATE TABLE IF NOT EXISTS dsa_core.refs (
    id SERIAL PRIMARY KEY,
    dataset_id INT NOT NULL REFERENCES dsa_core.datasets(id),
    name TEXT NOT NULL,
    commit_id CHAR(64) REFERENCES dsa_core.commits(commit_id),
    UNIQUE (dataset_id, name)
);

-- Insert test user if not exists
INSERT INTO dsa_auth.users (username, password_hash)
VALUES ('bg54677', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewKyNiGPJGYIG7yC')
ON CONFLICT (username) DO NOTHING;

-- Create test dataset
INSERT INTO dsa_core.datasets (id, name, description, created_by)
VALUES (1, 'Test Sales Data', 'Test dataset for SQL transform', 1)
ON CONFLICT (id) DO NOTHING;

-- Create initial commit with test data
INSERT INTO dsa_core.commits (commit_id, dataset_id, message, author_id)
VALUES ('initial_commit_hash', 1, 'Initial test data', 1)
ON CONFLICT (commit_id) DO NOTHING;

-- Insert test rows
INSERT INTO dsa_core.rows (row_hash, data) VALUES
('hash1', '{"sheet_name": "default", "row_number": 1, "data": {"id": 1, "product": "Widget", "price": 10.99, "quantity": 5}}'),
('hash2', '{"sheet_name": "default", "row_number": 2, "data": {"id": 2, "product": "Gadget", "price": 25.50, "quantity": 3}}'),
('hash3', '{"sheet_name": "default", "row_number": 3, "data": {"id": 3, "product": "Doohickey", "price": 15.00, "quantity": 7}}')
ON CONFLICT (row_hash) DO NOTHING;

-- Link rows to commit
INSERT INTO dsa_core.commit_rows (commit_id, logical_row_id, row_hash) VALUES
('initial_commit_hash', 'default:hash1', 'hash1'),
('initial_commit_hash', 'default:hash2', 'hash2'),
('initial_commit_hash', 'default:hash3', 'hash3')
ON CONFLICT DO NOTHING;

-- Create main ref
INSERT INTO dsa_core.refs (dataset_id, name, commit_id)
VALUES (1, 'main', 'initial_commit_hash')
ON CONFLICT (dataset_id, name) DO UPDATE SET commit_id = 'initial_commit_hash';

-- Verify setup
SELECT 'Test dataset created with ' || COUNT(*) || ' rows' 
FROM dsa_core.commit_rows 
WHERE commit_id = 'initial_commit_hash';
EOF

echo "2. Getting authentication token..."
TOKEN=$(curl -s -X POST http://localhost:8000/api/users/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=bg54677&password=password" | jq -r '.access_token')

if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
    echo "Failed to get token. Server may not be running."
    exit 1
fi

echo "Token obtained: ${TOKEN:0:20}..."

echo -e "\n3. Testing Preview Mode (save=false)..."
PREVIEW_RESPONSE=$(curl -s -X POST http://localhost:8000/api/workbench/sql-transform \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sources": [{
      "alias": "sales",
      "dataset_id": 1,
      "ref": "main",
      "table_key": "default"
    }],
    "sql": "SELECT data->>'\''id'\'' as id, data->>'\''product'\'' as product, (data->>'\''price'\'')::numeric * (data->>'\''quantity'\'')::numeric as total FROM sales",
    "save": false,
    "limit": 10
  }')

echo "Preview Response:"
echo "$PREVIEW_RESPONSE" | jq '.'

echo -e "\n4. Testing Save Mode (save=true) with transformation..."

# Get current commit for optimistic locking
CURRENT_COMMIT=$(PGPASSWORD=postgres psql -h localhost -U postgres -d postgres -t -c \
  "SELECT commit_id FROM dsa_core.refs WHERE dataset_id = 1 AND name = 'main'" | tr -d ' ')

echo "Current commit: $CURRENT_COMMIT"

TRANSFORM_RESPONSE=$(curl -s -X POST http://localhost:8000/api/workbench/sql-transform \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"sources\": [{
      \"alias\": \"sales\",
      \"dataset_id\": 1,
      \"ref\": \"main\",
      \"table_key\": \"default\"
    }],
    \"sql\": \"SELECT data->>'id' as id, data->>'product' as product, (data->>'price')::numeric * (data->>'quantity')::numeric as total_value FROM sales\",
    \"save\": true,
    \"target\": {
      \"dataset_id\": 1,
      \"ref\": \"main\",
      \"table_key\": \"sales_totals\",
      \"message\": \"Calculate total value for each product\",
      \"expected_head_commit_id\": \"$CURRENT_COMMIT\"
    }
  }")

echo "Transform Response:"
echo "$TRANSFORM_RESPONSE" | jq '.'

# Extract job ID
JOB_ID=$(echo "$TRANSFORM_RESPONSE" | jq -r '.job_id')

if [ -z "$JOB_ID" ] || [ "$JOB_ID" = "null" ]; then
    echo "Failed to create transformation job"
    exit 1
fi

echo -e "\n5. Checking job status..."
sleep 2  # Give job time to complete

JOB_STATUS=$(curl -s -X GET "http://localhost:8000/api/jobs/$JOB_ID" \
  -H "Authorization: Bearer $TOKEN")

echo "Job Status:"
echo "$JOB_STATUS" | jq '.'

# Check if job completed
STATUS=$(echo "$JOB_STATUS" | jq -r '.status')
if [ "$STATUS" = "completed" ]; then
    echo -e "\n✓ Job completed successfully!"
    
    # Verify the new commit was created
    echo -e "\n6. Verifying transformation results..."
    PGPASSWORD=postgres psql -h localhost -U postgres -d postgres << EOF
SELECT 'New commit created:' as info, 
       c.commit_id, 
       c.message,
       COUNT(cr.logical_row_id) as row_count
FROM dsa_core.commits c
JOIN dsa_core.commit_rows cr ON c.commit_id = cr.commit_id
WHERE c.dataset_id = 1 
  AND c.message = 'Calculate total value for each product'
  AND cr.logical_row_id LIKE 'sales_totals:%'
GROUP BY c.commit_id, c.message
ORDER BY c.committed_at DESC
LIMIT 1;

-- Show transformed data
SELECT 'Transformed data:' as info;
SELECT r.data->'data' as transformed_row
FROM dsa_core.commits c
JOIN dsa_core.commit_rows cr ON c.commit_id = cr.commit_id
JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
WHERE c.dataset_id = 1 
  AND c.message = 'Calculate total value for each product'
  AND cr.logical_row_id LIKE 'sales_totals:%'
LIMIT 5;
EOF
    
    echo -e "\n✓ All tests passed!"
else
    echo -e "\n✗ Job failed or is still running. Status: $STATUS"
    # Show job error if any
    ERROR=$(echo "$JOB_STATUS" | jq -r '.error_message // empty')
    if [ -n "$ERROR" ]; then
        echo "Error: $ERROR"
    fi
fi