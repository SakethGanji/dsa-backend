#!/bin/bash

# API base URL
BASE_URL="http://localhost:8000/api"

# User credentials
USERNAME="ng54677"
PASSWORD="string"

echo "=== Testing Exploration Endpoints ==="
echo

# 1. Get auth token
echo "1. Getting auth token..."
TOKEN_RESPONSE=$(curl -s -X POST "$BASE_URL/users/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=$USERNAME&password=$PASSWORD")

TOKEN=$(echo $TOKEN_RESPONSE | python3 -c "import sys, json; print(json.load(sys.stdin)['access_token'])")

if [ -z "$TOKEN" ]; then
    echo "Failed to get token. Response: $TOKEN_RESPONSE"
    exit 1
fi

echo "Token obtained successfully"
echo

# 2. Get available datasets
echo "2. Getting available datasets..."
DATASETS=$(curl -s -X GET "$BASE_URL/datasets" \
  -H "Authorization: Bearer $TOKEN")

echo "Datasets: $DATASETS"
DATASET_ID=$(echo $DATASETS | python3 -c "import sys, json; data=json.load(sys.stdin); print(data['datasets'][0]['id']) if data.get('datasets') else print('1')")
echo "Using dataset ID: $DATASET_ID"
echo

# 3. Create exploration job
echo "3. Creating exploration job..."
JOB_RESPONSE=$(curl -s -X POST "$BASE_URL/exploration/datasets/$DATASET_ID/jobs" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "source_ref": "main",
    "table_key": "primary",
    "profile_config": {
      "minimal": true,
      "samples_head": 5,
      "samples_tail": 5,
      "missing_diagrams": false,
      "correlation_threshold": 0.95
    }
  }')

echo "Job creation response: $JOB_RESPONSE"
JOB_ID=$(echo $JOB_RESPONSE | python3 -c "import sys, json; print(json.load(sys.stdin).get('job_id', ''))")
echo "Job ID: $JOB_ID"
echo

# 4. Check job status using generic job endpoint
echo "4. Checking job status..."
sleep 2
JOB_STATUS=$(curl -s -X GET "$BASE_URL/jobs/$JOB_ID" \
  -H "Authorization: Bearer $TOKEN")

echo "Job status: $JOB_STATUS"
echo

# 5. Get dataset exploration history
echo "5. Getting dataset exploration history..."
DATASET_HISTORY=$(curl -s -X GET "$BASE_URL/exploration/datasets/$DATASET_ID/history?offset=0&limit=10" \
  -H "Authorization: Bearer $TOKEN")

echo "Dataset exploration history: $DATASET_HISTORY"
echo

# 6. Get user exploration history
echo "6. Getting user exploration history..."
USER_ID=$(echo $TOKEN_RESPONSE | python3 -c "import sys, json; print(json.load(sys.stdin).get('user_id', '1'))")
USER_HISTORY=$(curl -s -X GET "$BASE_URL/exploration/users/$USER_ID/history?offset=0&limit=10" \
  -H "Authorization: Bearer $TOKEN")

echo "User exploration history: $USER_HISTORY"
echo

# 7. Wait for job to complete and get result
echo "7. Waiting for job to complete..."
for i in {1..30}; do
    JOB_STATUS=$(curl -s -X GET "$BASE_URL/jobs/$JOB_ID" \
      -H "Authorization: Bearer $TOKEN")
    
    STATUS=$(echo $JOB_STATUS | python3 -c "import sys, json; print(json.load(sys.stdin).get('status', ''))")
    
    if [ "$STATUS" = "completed" ]; then
        echo "Job completed!"
        break
    elif [ "$STATUS" = "failed" ]; then
        echo "Job failed!"
        echo "Error: $(echo $JOB_STATUS | python3 -c "import sys, json; print(json.load(sys.stdin).get('error_message', ''))")"
        exit 1
    fi
    
    echo "Status: $STATUS (attempt $i/30)"
    sleep 2
done
echo

# 8. Get exploration result in different formats
echo "8. Getting exploration results..."

# Get dataset info
echo "Getting dataset info..."
INFO_RESULT=$(curl -s -X GET "$BASE_URL/exploration/jobs/$JOB_ID/result?format=info" \
  -H "Authorization: Bearer $TOKEN")
echo "Dataset info: $INFO_RESULT"
echo

# Get JSON result (first 500 chars)
echo "Getting JSON result preview..."
JSON_RESULT=$(curl -s -X GET "$BASE_URL/exploration/jobs/$JOB_ID/result?format=json" \
  -H "Authorization: Bearer $TOKEN" | head -c 500)
echo "JSON result preview: $JSON_RESULT..."
echo

# Get HTML result (check if it exists)
echo "Getting HTML result..."
HTML_RESULT=$(curl -s -I -X GET "$BASE_URL/exploration/jobs/$JOB_ID/result?format=html" \
  -H "Authorization: Bearer $TOKEN")
echo "HTML result headers: $HTML_RESULT"

echo
echo "=== Test Complete ===