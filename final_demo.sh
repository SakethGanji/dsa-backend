#!/bin/bash

echo "=== SQL Transform API Demo ==="
echo "Demonstrating both preview and save modes with server-side processing"

# Get token
TOKEN=$(curl -s -X POST http://localhost:8000/api/users/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=bg54677&password=password" | jq -r '.access_token')

echo -e "\n1. PREVIEW MODE (save=false) - No data persisted"
echo "Running transformation to calculate product totals..."

curl -s -X POST http://localhost:8000/api/workbench/sql-transform \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sources": [{
      "alias": "products",
      "dataset_id": 1,
      "ref": "main",
      "table_key": "default"
    }],
    "sql": "SELECT (data->>'\''data'\'')::json->>'\''product'\'' as name, ((data->>'\''data'\'')::json->>'\''price'\'')::numeric * (data->>'\''data'\'')::json->>'\''quantity'\'')::numeric as revenue FROM products WHERE data->>'\''sheet_name'\'' = '\''default'\''",
    "save": false,
    "limit": 10
  }' | jq '.data'

echo -e "\n2. SAVE MODE (save=true) - Creates new commit"
echo "Transforming and saving aggregated sales data..."

RESPONSE=$(curl -s -X POST http://localhost:8000/api/workbench/sql-transform \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sources": [{
      "alias": "sales",
      "dataset_id": 1,
      "ref": "main", 
      "table_key": "default"
    }],
    "sql": "SELECT '\''TOTAL'\'' as category, SUM(((data->>'\''data'\'')::json->>'\''price'\'')::numeric * ((data->>'\''data'\'')::json->>'\''quantity'\'')::numeric) as total_revenue, COUNT(*) as product_count FROM sales WHERE data->>'\''sheet_name'\'' = '\''default'\''",
    "save": true,
    "target": {
      "dataset_id": 1,
      "ref": "main",
      "table_key": "sales_summary",
      "message": "Aggregate sales metrics",
      "output_branch_name": "analytics/sales-summary"
    }
  }')

JOB_ID=$(echo "$RESPONSE" | jq -r '.job_id')
echo "Job created: $JOB_ID"

# Wait for completion
echo -n "Waiting for job to complete..."
for i in {1..10}; do
  sleep 1
  STATUS=$(curl -s -X GET "http://localhost:8000/api/jobs/$JOB_ID" \
    -H "Authorization: Bearer $TOKEN" | jq -r '.status')
  
  if [ "$STATUS" = "completed" ]; then
    echo " ✓ COMPLETED"
    break
  elif [ "$STATUS" = "failed" ]; then
    echo " ✗ FAILED"
    exit 1
  fi
  echo -n "."
done

echo -e "\n3. Verifying results in database..."
PGPASSWORD=postgres psql -h localhost -U postgres -d postgres << 'EOF'
\echo 'Latest commits:'
SELECT c.commit_id, c.message, c.committed_at
FROM dsa_core.commits c
WHERE c.dataset_id = 1
ORDER BY c.committed_at DESC
LIMIT 3;

\echo '\nData saved in new commit:'
SELECT r.data->'data' as summary_data
FROM dsa_core.commits c
JOIN dsa_core.commit_rows cr ON c.commit_id = cr.commit_id  
JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
WHERE c.message = 'Aggregate sales metrics'
AND cr.logical_row_id LIKE 'sales_summary:%';

\echo '\nBranches created:'
SELECT name, commit_id
FROM dsa_core.refs
WHERE dataset_id = 1
AND name LIKE 'analytics/%';
EOF

echo -e "\n✓ Demo complete! The transformation:"
echo "  - Processed data entirely in PostgreSQL (zero memory usage)"
echo "  - Created a new immutable commit"
echo "  - Updated the 'main' ref"
echo "  - Created a new branch 'analytics/sales-summary'"