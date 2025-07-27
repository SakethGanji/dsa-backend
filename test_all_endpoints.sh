#!/bin/bash

# Test script for all API endpoints
# Validates API functionality after refactoring

# Configuration
BASE_URL="http://localhost:8000/api"
USERNAME="bg54677"
PASSWORD="string"
TOKEN=""
DATASET_ID=""
JOB_ID=""
SAMPLING_JOB_ID=""
EXPLORATION_JOB_ID=""
REF_NAME="test-branch"
COMMIT_ID=""
TABLE_KEY=""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print test results
print_test() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓ $2${NC}"
    else
        echo -e "${RED}✗ $2${NC}"
        echo "Response: $3"
    fi
}

# Function to extract value from JSON
get_json_value() {
    echo "$1" | grep -o "\"$2\":[^,}]*" | cut -d':' -f2 | tr -d '"' | tr -d ' '
}

echo "=== DSA API Endpoint Testing ==="
echo "================================"

# 1. Test Authentication
echo -e "\n${YELLOW}1. Testing Authentication Endpoints${NC}"

# POST /users/login
echo "Testing POST /users/login..."
response=$(curl -s -X POST "$BASE_URL/users/login" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "username=$USERNAME&password=$PASSWORD" \
    -w "\n%{http_code}")
http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | head -n-1)
TOKEN=$(get_json_value "$body" "access_token")

if [ "$http_code" = "200" ] && [ -n "$TOKEN" ]; then
    print_test 0 "POST /users/login"
else
    print_test 1 "POST /users/login" "$body"
    echo "Cannot continue without authentication token"
    exit 1
fi

# GET /users/register-public (test public registration endpoint)
echo "Testing POST /users/register-public..."
response=$(curl -s -X POST "$BASE_URL/users/register-public" \
    -H "Content-Type: application/json" \
    -d '{
        "username": "test-user-'$(date +%s)'",
        "email": "test'$(date +%s)'@example.com",
        "password": "testpassword123"
    }' \
    -w "\n%{http_code}")
http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | head -n-1)
[ "$http_code" = "200" ] && print_test 0 "POST /users/register-public" || print_test 1 "POST /users/register-public" "$body"

# 2. Test Dataset CRUD Endpoints
echo -e "\n${YELLOW}2. Testing Dataset CRUD Endpoints${NC}"

# POST /datasets/
echo "Testing POST /datasets/..."
response=$(curl -s -X POST "$BASE_URL/datasets/" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{
        "name": "test-dataset-'$(date +%s)'",
        "description": "Test dataset for API validation",
        "tags": ["test", "validation"]
    }' \
    -w "\n%{http_code}")
http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | head -n-1)
DATASET_ID=$(get_json_value "$body" "dataset_id")

if [ "$http_code" = "200" ] && [ -n "$DATASET_ID" ]; then
    print_test 0 "POST /datasets/"
else
    print_test 1 "POST /datasets/" "$body"
fi

# POST /datasets/create-with-file
echo "Testing POST /datasets/create-with-file..."
# Create a test CSV file
TEST_FILE="/tmp/test-data-$(date +%s).csv"
echo "id,name,value" > $TEST_FILE
echo "1,test1,100" >> $TEST_FILE
echo "2,test2,200" >> $TEST_FILE

response=$(curl -s -X POST "$BASE_URL/datasets/create-with-file" \
    -H "Authorization: Bearer $TOKEN" \
    -F "file=@$TEST_FILE" \
    -F 'dataset_info={"name":"test-file-dataset-'$(date +%s)'","description":"Test dataset created with file"}' \
    -w "\n%{http_code}")
http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | head -n-1)
rm -f $TEST_FILE

if [ "$http_code" = "200" ]; then
    print_test 0 "POST /datasets/create-with-file"
else
    print_test 1 "POST /datasets/create-with-file" "$body"
fi

# GET /datasets/
echo "Testing GET /datasets/..."
response=$(curl -s -X GET "$BASE_URL/datasets/" \
    -H "Authorization: Bearer $TOKEN" \
    -w "\n%{http_code}")
http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | head -n-1)
[ "$http_code" = "200" ] && print_test 0 "GET /datasets/" || print_test 1 "GET /datasets/" "$body"

if [ -n "$DATASET_ID" ]; then
    # GET /datasets/{dataset_id}
    echo "Testing GET /datasets/$DATASET_ID..."
    response=$(curl -s -X GET "$BASE_URL/datasets/$DATASET_ID" \
        -H "Authorization: Bearer $TOKEN" \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "GET /datasets/{dataset_id}" || print_test 1 "GET /datasets/{dataset_id}" "$body"

    # PATCH /datasets/{dataset_id}
    echo "Testing PATCH /datasets/$DATASET_ID..."
    response=$(curl -s -X PATCH "$BASE_URL/datasets/$DATASET_ID" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d '{
            "description": "Updated description",
            "tags": ["test", "updated"]
        }' \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "PATCH /datasets/{dataset_id}" || print_test 1 "PATCH /datasets/{dataset_id}" "$body"

    # POST /datasets/{dataset_id}/permissions
    echo "Testing POST /datasets/$DATASET_ID/permissions..."
    response=$(curl -s -X POST "$BASE_URL/datasets/$DATASET_ID/permissions" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d '{
            "user_id": 1,
            "permission": "read"
        }' \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "POST /datasets/{dataset_id}/permissions" || print_test 1 "POST /datasets/{dataset_id}/permissions" "$body"

    # GET /datasets/{dataset_id}/ready
    echo "Testing GET /datasets/$DATASET_ID/ready..."
    response=$(curl -s -X GET "$BASE_URL/datasets/$DATASET_ID/ready" \
        -H "Authorization: Bearer $TOKEN" \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "GET /datasets/{dataset_id}/ready" || print_test 1 "GET /datasets/{dataset_id}/ready" "$body"
fi

# 3. Test Versioning Endpoints
echo -e "\n${YELLOW}3. Testing Versioning Endpoints${NC}"

if [ -n "$DATASET_ID" ]; then
    # GET /datasets/{dataset_id}/refs
    echo "Testing GET /datasets/$DATASET_ID/refs..."
    response=$(curl -s -X GET "$BASE_URL/datasets/$DATASET_ID/refs" \
        -H "Authorization: Bearer $TOKEN" \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "GET /datasets/{dataset_id}/refs" || print_test 1 "GET /datasets/{dataset_id}/refs" "$body"

    # POST /datasets/{dataset_id}/refs
    echo "Testing POST /datasets/$DATASET_ID/refs..."
    response=$(curl -s -X POST "$BASE_URL/datasets/$DATASET_ID/refs" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d '{
            "name": "'$REF_NAME'",
            "from_ref": "main"
        }' \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "POST /datasets/{dataset_id}/refs" || print_test 1 "POST /datasets/{dataset_id}/refs" "$body"

    # GET /datasets/{dataset_id}/history
    echo "Testing GET /datasets/$DATASET_ID/history..."
    response=$(curl -s -X GET "$BASE_URL/datasets/$DATASET_ID/history" \
        -H "Authorization: Bearer $TOKEN" \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "GET /datasets/{dataset_id}/history" || print_test 1 "GET /datasets/{dataset_id}/history" "$body"
    
    # Extract first commit ID if any
    COMMIT_ID=$(echo "$body" | grep -o '"commit_id":"[^"]*"' | head -1 | cut -d'"' -f4)

    # GET /datasets/{dataset_id}/overview
    echo "Testing GET /datasets/$DATASET_ID/overview..."
    response=$(curl -s -X GET "$BASE_URL/datasets/$DATASET_ID/overview" \
        -H "Authorization: Bearer $TOKEN" \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "GET /datasets/{dataset_id}/overview" || print_test 1 "GET /datasets/{dataset_id}/overview" "$body"

    # POST /datasets/{dataset_id}/refs/{ref_name}/commits
    echo "Testing POST /datasets/$DATASET_ID/refs/main/commits..."
    response=$(curl -s -X POST "$BASE_URL/datasets/$DATASET_ID/refs/main/commits" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d '{
            "message": "Test commit",
            "author": "test@example.com"
        }' \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "POST /datasets/{dataset_id}/refs/{ref_name}/commits" || print_test 1 "POST /datasets/{dataset_id}/refs/{ref_name}/commits" "$body"

    # GET /datasets/{dataset_id}/refs/{ref_name}/data
    echo "Testing GET /datasets/$DATASET_ID/refs/main/data..."
    response=$(curl -s -X GET "$BASE_URL/datasets/$DATASET_ID/refs/main/data" \
        -H "Authorization: Bearer $TOKEN" \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "GET /datasets/{dataset_id}/refs/{ref_name}/data" || print_test 1 "GET /datasets/{dataset_id}/refs/{ref_name}/data" "$body"

    # GET /datasets/{dataset_id}/refs/{ref_name}/tables
    echo "Testing GET /datasets/$DATASET_ID/refs/main/tables..."
    response=$(curl -s -X GET "$BASE_URL/datasets/$DATASET_ID/refs/main/tables" \
        -H "Authorization: Bearer $TOKEN" \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "GET /datasets/{dataset_id}/refs/{ref_name}/tables" || print_test 1 "GET /datasets/{dataset_id}/refs/{ref_name}/tables" "$body"
    
    # Extract table key if any
    TABLE_KEY=$(echo "$body" | grep -o '"table_key":"[^"]*"' | head -1 | cut -d'"' -f4)

    if [ -n "$TABLE_KEY" ]; then
        # GET /datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/data
        echo "Testing GET /datasets/$DATASET_ID/refs/main/tables/$TABLE_KEY/data..."
        response=$(curl -s -X GET "$BASE_URL/datasets/$DATASET_ID/refs/main/tables/$TABLE_KEY/data" \
            -H "Authorization: Bearer $TOKEN" \
            -w "\n%{http_code}")
        http_code=$(echo "$response" | tail -n1)
        body=$(echo "$response" | head -n-1)
        [ "$http_code" = "200" ] && print_test 0 "GET /datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/data" || print_test 1 "GET /datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/data" "$body"

        # GET /datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/schema
        echo "Testing GET /datasets/$DATASET_ID/refs/main/tables/$TABLE_KEY/schema..."
        response=$(curl -s -X GET "$BASE_URL/datasets/$DATASET_ID/refs/main/tables/$TABLE_KEY/schema" \
            -H "Authorization: Bearer $TOKEN" \
            -w "\n%{http_code}")
        http_code=$(echo "$response" | tail -n1)
        body=$(echo "$response" | head -n-1)
        [ "$http_code" = "200" ] && print_test 0 "GET /datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/schema" || print_test 1 "GET /datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/schema" "$body"

        # GET /datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/analysis
        echo "Testing GET /datasets/$DATASET_ID/refs/main/tables/$TABLE_KEY/analysis..."
        response=$(curl -s -X GET "$BASE_URL/datasets/$DATASET_ID/refs/main/tables/$TABLE_KEY/analysis" \
            -H "Authorization: Bearer $TOKEN" \
            -w "\n%{http_code}")
        http_code=$(echo "$response" | tail -n1)
        body=$(echo "$response" | head -n-1)
        [ "$http_code" = "200" ] && print_test 0 "GET /datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/analysis" || print_test 1 "GET /datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/analysis" "$body"
    fi

    if [ -n "$COMMIT_ID" ]; then
        # GET /datasets/{dataset_id}/commits/{commit_id}/schema
        echo "Testing GET /datasets/$DATASET_ID/commits/$COMMIT_ID/schema..."
        response=$(curl -s -X GET "$BASE_URL/datasets/$DATASET_ID/commits/$COMMIT_ID/schema" \
            -H "Authorization: Bearer $TOKEN" \
            -w "\n%{http_code}")
        http_code=$(echo "$response" | tail -n1)
        body=$(echo "$response" | head -n-1)
        [ "$http_code" = "200" ] && print_test 0 "GET /datasets/{dataset_id}/commits/{commit_id}/schema" || print_test 1 "GET /datasets/{dataset_id}/commits/{commit_id}/schema" "$body"

        # GET /datasets/{dataset_id}/commits/{commit_id}/data
        echo "Testing GET /datasets/$DATASET_ID/commits/$COMMIT_ID/data..."
        response=$(curl -s -X GET "$BASE_URL/datasets/$DATASET_ID/commits/$COMMIT_ID/data" \
            -H "Authorization: Bearer $TOKEN" \
            -w "\n%{http_code}")
        http_code=$(echo "$response" | tail -n1)
        body=$(echo "$response" | head -n-1)
        [ "$http_code" = "200" ] && print_test 0 "GET /datasets/{dataset_id}/commits/{commit_id}/data" || print_test 1 "GET /datasets/{dataset_id}/commits/{commit_id}/data" "$body"
    fi

    # POST /datasets/{dataset_id}/refs/{ref_name}/import
    echo "Testing POST /datasets/$DATASET_ID/refs/main/import..."
    response=$(curl -s -X POST "$BASE_URL/datasets/$DATASET_ID/refs/main/import" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d '{
            "source_type": "file",
            "source_path": "/tmp/test.csv"
        }' \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "POST /datasets/{dataset_id}/refs/{ref_name}/import" || print_test 1 "POST /datasets/{dataset_id}/refs/{ref_name}/import" "$body"

    # DELETE /datasets/{dataset_id}/refs/{ref_name}
    if [ -n "$REF_NAME" ]; then
        echo "Testing DELETE /datasets/$DATASET_ID/refs/$REF_NAME..."
        response=$(curl -s -X DELETE "$BASE_URL/datasets/$DATASET_ID/refs/$REF_NAME" \
            -H "Authorization: Bearer $TOKEN" \
            -w "\n%{http_code}")
        http_code=$(echo "$response" | tail -n1)
        body=$(echo "$response" | head -n-1)
        [ "$http_code" = "200" ] && print_test 0 "DELETE /datasets/{dataset_id}/refs/{ref_name}" || print_test 1 "DELETE /datasets/{dataset_id}/refs/{ref_name}" "$body"
    fi
fi

# 4. Test Job Endpoints
echo -e "\n${YELLOW}4. Testing Job Endpoints${NC}"

# GET /jobs
echo "Testing GET /jobs..."
response=$(curl -s -X GET "$BASE_URL/jobs" \
    -H "Authorization: Bearer $TOKEN" \
    -w "\n%{http_code}")
http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | head -n-1)
[ "$http_code" = "200" ] && print_test 0 "GET /jobs" || print_test 1 "GET /jobs" "$body"

# Extract first job ID if any
JOB_ID=$(echo "$body" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)

if [ -n "$JOB_ID" ]; then
    # GET /jobs/{job_id}
    echo "Testing GET /jobs/$JOB_ID..."
    response=$(curl -s -X GET "$BASE_URL/jobs/$JOB_ID" \
        -H "Authorization: Bearer $TOKEN" \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "GET /jobs/{job_id}" || print_test 1 "GET /jobs/{job_id}" "$body"
fi

# 5. Test Search Endpoints
echo -e "\n${YELLOW}5. Testing Search Endpoints${NC}"

# GET /datasets/search/
echo "Testing GET /datasets/search/..."
response=$(curl -s -X GET "$BASE_URL/datasets/search/?q=test" \
    -H "Authorization: Bearer $TOKEN" \
    -w "\n%{http_code}")
http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | head -n-1)
[ "$http_code" = "200" ] && print_test 0 "GET /datasets/search/" || print_test 1 "GET /datasets/search/" "$body"

# GET /datasets/search/suggest
echo "Testing GET /datasets/search/suggest..."
response=$(curl -s -X GET "$BASE_URL/datasets/search/suggest?query=test" \
    -H "Authorization: Bearer $TOKEN" \
    -w "\n%{http_code}")
http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | head -n-1)
[ "$http_code" = "200" ] && print_test 0 "GET /datasets/search/suggest" || print_test 1 "GET /datasets/search/suggest" "$body"

# 6. Test Sampling Endpoints (if dataset exists)
echo -e "\n${YELLOW}6. Testing Sampling Endpoints${NC}"

if [ -n "$DATASET_ID" ]; then
    # GET /sampling/datasets/{dataset_id}/sampling-methods
    echo "Testing GET /sampling/datasets/$DATASET_ID/sampling-methods..."
    response=$(curl -s -X GET "$BASE_URL/sampling/datasets/$DATASET_ID/sampling-methods" \
        -H "Authorization: Bearer $TOKEN" \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "GET /sampling/datasets/{dataset_id}/sampling-methods" || print_test 1 "GET /sampling/datasets/{dataset_id}/sampling-methods" "$body"

    # GET /sampling/datasets/{dataset_id}/history
    echo "Testing GET /sampling/datasets/$DATASET_ID/history..."
    response=$(curl -s -X GET "$BASE_URL/sampling/datasets/$DATASET_ID/history" \
        -H "Authorization: Bearer $TOKEN" \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "GET /sampling/datasets/{dataset_id}/history" || print_test 1 "GET /sampling/datasets/{dataset_id}/history" "$body"

    # POST /sampling/datasets/{dataset_id}/jobs
    echo "Testing POST /sampling/datasets/$DATASET_ID/jobs..."
    response=$(curl -s -X POST "$BASE_URL/sampling/datasets/$DATASET_ID/jobs" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d '{
            "sampling_method": "random",
            "parameters": {"sample_size": 100}
        }' \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    SAMPLING_JOB_ID=$(get_json_value "$body" "job_id")
    [ "$http_code" = "200" ] && print_test 0 "POST /sampling/datasets/{dataset_id}/jobs" || print_test 1 "POST /sampling/datasets/{dataset_id}/jobs" "$body"

    # POST /sampling/datasets/{dataset_id}/sample
    echo "Testing POST /sampling/datasets/$DATASET_ID/sample..."
    response=$(curl -s -X POST "$BASE_URL/sampling/datasets/$DATASET_ID/sample" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d '{
            "sampling_method": "random",
            "parameters": {"sample_size": 10}
        }' \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "POST /sampling/datasets/{dataset_id}/sample" || print_test 1 "POST /sampling/datasets/{dataset_id}/sample" "$body"

    # POST /sampling/datasets/{dataset_id}/column-samples
    echo "Testing POST /sampling/datasets/$DATASET_ID/column-samples..."
    response=$(curl -s -X POST "$BASE_URL/sampling/datasets/$DATASET_ID/column-samples" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d '{
            "columns": ["column1", "column2"],
            "sample_size": 10
        }' \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "POST /sampling/datasets/{dataset_id}/column-samples" || print_test 1 "POST /sampling/datasets/{dataset_id}/column-samples" "$body"

    if [ -n "$SAMPLING_JOB_ID" ]; then
        # GET /sampling/jobs/{job_id}/data
        echo "Testing GET /sampling/jobs/$SAMPLING_JOB_ID/data..."
        response=$(curl -s -X GET "$BASE_URL/sampling/jobs/$SAMPLING_JOB_ID/data" \
            -H "Authorization: Bearer $TOKEN" \
            -w "\n%{http_code}")
        http_code=$(echo "$response" | tail -n1)
        body=$(echo "$response" | head -n-1)
        [ "$http_code" = "200" ] && print_test 0 "GET /sampling/jobs/{job_id}/data" || print_test 1 "GET /sampling/jobs/{job_id}/data" "$body"

        # GET /sampling/jobs/{job_id}/residual
        echo "Testing GET /sampling/jobs/$SAMPLING_JOB_ID/residual..."
        response=$(curl -s -X GET "$BASE_URL/sampling/jobs/$SAMPLING_JOB_ID/residual" \
            -H "Authorization: Bearer $TOKEN" \
            -w "\n%{http_code}")
        http_code=$(echo "$response" | tail -n1)
        body=$(echo "$response" | head -n-1)
        [ "$http_code" = "200" ] && print_test 0 "GET /sampling/jobs/{job_id}/residual" || print_test 1 "GET /sampling/jobs/{job_id}/residual" "$body"
    fi
fi

# GET /sampling/users/{user_id}/history (get current user ID from token)
echo "Testing GET /sampling/users/me/history..."
# First try to get current user ID - if this fails, we'll skip this test
USER_ID=$(echo "$TOKEN" | cut -d'.' -f2 | base64 -d 2>/dev/null | grep -o '"user_id":[^,}]*' | cut -d':' -f2 | tr -d ' ')
if [ -n "$USER_ID" ]; then
    response=$(curl -s -X GET "$BASE_URL/sampling/users/$USER_ID/history" \
        -H "Authorization: Bearer $TOKEN" \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "GET /sampling/users/{user_id}/history" || print_test 1 "GET /sampling/users/{user_id}/history" "$body"
else
    echo "Skipping GET /sampling/users/{user_id}/history - couldn't extract user ID"
fi

# 7. Test Workbench Endpoints
echo -e "\n${YELLOW}7. Testing Workbench Endpoints${NC}"

# POST /workbench/sql-preview
echo "Testing POST /workbench/sql-preview..."
response=$(curl -s -X POST "$BASE_URL/workbench/sql-preview" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"query": "SELECT 1 as test", "limit": 10}' \
    -w "\n%{http_code}")
http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | head -n-1)
[ "$http_code" = "200" ] && print_test 0 "POST /workbench/sql-preview" || print_test 1 "POST /workbench/sql-preview" "$body"

# POST /workbench/sql-transform
echo "Testing POST /workbench/sql-transform..."
response=$(curl -s -X POST "$BASE_URL/workbench/sql-transform" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{
        "dataset_id": "'$DATASET_ID'",
        "query": "SELECT * FROM data LIMIT 10",
        "output_name": "transformed_data"
    }' \
    -w "\n%{http_code}")
http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | head -n-1)
[ "$http_code" = "200" ] && print_test 0 "POST /workbench/sql-transform" || print_test 1 "POST /workbench/sql-transform" "$body"

# 8. Test Exploration Endpoints
echo -e "\n${YELLOW}8. Testing Exploration Endpoints${NC}"

if [ -n "$DATASET_ID" ]; then
    # POST /exploration/datasets/{dataset_id}/jobs
    echo "Testing POST /exploration/datasets/$DATASET_ID/jobs..."
    response=$(curl -s -X POST "$BASE_URL/exploration/datasets/$DATASET_ID/jobs" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d '{
            "exploration_type": "summary",
            "parameters": {}
        }' \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    EXPLORATION_JOB_ID=$(get_json_value "$body" "job_id")
    [ "$http_code" = "200" ] && print_test 0 "POST /exploration/datasets/{dataset_id}/jobs" || print_test 1 "POST /exploration/datasets/{dataset_id}/jobs" "$body"

    # GET /exploration/datasets/{dataset_id}/history
    echo "Testing GET /exploration/datasets/$DATASET_ID/history..."
    response=$(curl -s -X GET "$BASE_URL/exploration/datasets/$DATASET_ID/history" \
        -H "Authorization: Bearer $TOKEN" \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "GET /exploration/datasets/{dataset_id}/history" || print_test 1 "GET /exploration/datasets/{dataset_id}/history" "$body"

    if [ -n "$EXPLORATION_JOB_ID" ]; then
        # GET /exploration/jobs/{job_id}/result
        echo "Testing GET /exploration/jobs/$EXPLORATION_JOB_ID/result..."
        response=$(curl -s -X GET "$BASE_URL/exploration/jobs/$EXPLORATION_JOB_ID/result" \
            -H "Authorization: Bearer $TOKEN" \
            -w "\n%{http_code}")
        http_code=$(echo "$response" | tail -n1)
        body=$(echo "$response" | head -n-1)
        [ "$http_code" = "200" ] && print_test 0 "GET /exploration/jobs/{job_id}/result" || print_test 1 "GET /exploration/jobs/{job_id}/result" "$body"
    fi
fi

# GET /exploration/users/{user_id}/history
if [ -n "$USER_ID" ]; then
    echo "Testing GET /exploration/users/$USER_ID/history..."
    response=$(curl -s -X GET "$BASE_URL/exploration/users/$USER_ID/history" \
        -H "Authorization: Bearer $TOKEN" \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "GET /exploration/users/{user_id}/history" || print_test 1 "GET /exploration/users/{user_id}/history" "$body"
else
    echo "Skipping GET /exploration/users/{user_id}/history - no user ID available"
fi

# 9. Test Download Endpoints
echo -e "\n${YELLOW}9. Testing Download Endpoints${NC}"

if [ -n "$DATASET_ID" ]; then
    # GET /datasets/{dataset_id}/refs/{ref_name}/download
    echo "Testing GET /datasets/$DATASET_ID/refs/main/download..."
    response=$(curl -s -X GET "$BASE_URL/datasets/$DATASET_ID/refs/main/download" \
        -H "Authorization: Bearer $TOKEN" \
        -w "\n%{http_code}" \
        -o /dev/null)
    http_code=$(echo "$response" | tail -n1)
    [ "$http_code" = "200" ] && print_test 0 "GET /datasets/{dataset_id}/refs/{ref_name}/download" || print_test 1 "GET /datasets/{dataset_id}/refs/{ref_name}/download" "HTTP $http_code"

    if [ -n "$TABLE_KEY" ]; then
        # GET /datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/download
        echo "Testing GET /datasets/$DATASET_ID/refs/main/tables/$TABLE_KEY/download..."
        response=$(curl -s -X GET "$BASE_URL/datasets/$DATASET_ID/refs/main/tables/$TABLE_KEY/download" \
            -H "Authorization: Bearer $TOKEN" \
            -w "\n%{http_code}" \
            -o /dev/null)
        http_code=$(echo "$response" | tail -n1)
        [ "$http_code" = "200" ] && print_test 0 "GET /datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/download" || print_test 1 "GET /datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/download" "HTTP $http_code"
    fi
fi

# 10. Clean up - Delete test dataset
echo -e "\n${YELLOW}10. Cleanup${NC}"

if [ -n "$DATASET_ID" ]; then
    echo "Testing DELETE /datasets/$DATASET_ID..."
    response=$(curl -s -X DELETE "$BASE_URL/datasets/$DATASET_ID" \
        -H "Authorization: Bearer $TOKEN" \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "DELETE /datasets/{dataset_id}" || print_test 1 "DELETE /datasets/{dataset_id}" "$body"
fi

echo -e "\n${YELLOW}=== Testing Complete ===${NC}"