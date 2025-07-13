#!/bin/bash

# Test script for all affected endpoints
# Ensures behavior is exactly the same after refactoring

# Configuration
BASE_URL="http://localhost:8000/api"
USERNAME="bg54677"
PASSWORD="string"
TOKEN=""
DATASET_ID=""
JOB_ID=""
REF_NAME="test-branch"

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

# GET /users/me
echo "Testing GET /users/me..."
response=$(curl -s -X GET "$BASE_URL/users/me" \
    -H "Authorization: Bearer $TOKEN" \
    -w "\n%{http_code}")
http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | head -n-1)
[ "$http_code" = "200" ] && print_test 0 "GET /users/me" || print_test 1 "GET /users/me" "$body"

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

    # PUT /datasets/{dataset_id}
    echo "Testing PUT /datasets/$DATASET_ID..."
    response=$(curl -s -X PUT "$BASE_URL/datasets/$DATASET_ID" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d '{
            "name": "updated-test-dataset",
            "description": "Fully updated dataset"
        }' \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "PUT /datasets/{dataset_id}" || print_test 1 "PUT /datasets/{dataset_id}" "$body"
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

    # GET /datasets/{dataset_id}/commits
    echo "Testing GET /datasets/$DATASET_ID/commits..."
    response=$(curl -s -X GET "$BASE_URL/datasets/$DATASET_ID/commits" \
        -H "Authorization: Bearer $TOKEN" \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "GET /datasets/{dataset_id}/commits" || print_test 1 "GET /datasets/{dataset_id}/commits" "$body"

    # GET /datasets/{dataset_id}/refs/{ref_name}
    echo "Testing GET /datasets/$DATASET_ID/refs/main..."
    response=$(curl -s -X GET "$BASE_URL/datasets/$DATASET_ID/refs/main" \
        -H "Authorization: Bearer $TOKEN" \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "GET /datasets/{dataset_id}/refs/{ref_name}" || print_test 1 "GET /datasets/{dataset_id}/refs/{ref_name}" "$body"

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

    # GET /jobs/{job_id}/status
    echo "Testing GET /jobs/$JOB_ID/status..."
    response=$(curl -s -X GET "$BASE_URL/jobs/$JOB_ID/status" \
        -H "Authorization: Bearer $TOKEN" \
        -w "\n%{http_code}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    [ "$http_code" = "200" ] && print_test 0 "GET /jobs/{job_id}/status" || print_test 1 "GET /jobs/{job_id}/status" "$body"
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

# GET /datasets/search/tags
echo "Testing GET /datasets/search/tags..."
response=$(curl -s -X GET "$BASE_URL/datasets/search/tags" \
    -H "Authorization: Bearer $TOKEN" \
    -w "\n%{http_code}")
http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | head -n-1)
[ "$http_code" = "200" ] && print_test 0 "GET /datasets/search/tags" || print_test 1 "GET /datasets/search/tags" "$body"

# GET /datasets/search/users
echo "Testing GET /datasets/search/users..."
response=$(curl -s -X GET "$BASE_URL/datasets/search/users?q=bg" \
    -H "Authorization: Bearer $TOKEN" \
    -w "\n%{http_code}")
http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | head -n-1)
[ "$http_code" = "200" ] && print_test 0 "GET /datasets/search/users" || print_test 1 "GET /datasets/search/users" "$body"

# POST /datasets/search/refresh
echo "Testing POST /datasets/search/refresh..."
response=$(curl -s -X POST "$BASE_URL/datasets/search/refresh" \
    -H "Authorization: Bearer $TOKEN" \
    -w "\n%{http_code}")
http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | head -n-1)
[ "$http_code" = "200" ] && print_test 0 "POST /datasets/search/refresh" || print_test 1 "POST /datasets/search/refresh" "$body"

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
fi

# GET /sampling/users/{user_id}/history (using current user)
echo "Testing GET /sampling/users/87/history..."
response=$(curl -s -X GET "$BASE_URL/sampling/users/87/history" \
    -H "Authorization: Bearer $TOKEN" \
    -w "\n%{http_code}")
http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | head -n-1)
[ "$http_code" = "200" ] && print_test 0 "GET /sampling/users/{user_id}/history" || print_test 1 "GET /sampling/users/{user_id}/history" "$body"

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

# 8. Clean up - Delete test dataset
echo -e "\n${YELLOW}8. Cleanup${NC}"

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