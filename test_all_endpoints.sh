#!/bin/bash

# Test script for all API endpoints

BASE_URL="http://localhost:8000"

echo "=== DSA Platform API Test ==="
echo

# 1. Health Check
echo "1. Testing Health Check"
curl -s $BASE_URL/health | python3 -m json.tool
echo

# 2. Root Endpoint
echo "2. Testing Root Endpoint"
curl -s $BASE_URL/ | python3 -m json.tool
echo

# 3. Login
echo "3. Testing User Login"
RESPONSE=$(curl -s -X POST $BASE_URL/api/users/login \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=TEST001&password=testpass")
echo $RESPONSE | python3 -m json.tool

# Extract token
TOKEN=$(echo $RESPONSE | python3 -c "import sys, json; print(json.load(sys.stdin)['access_token'])")
echo "Token acquired: ${TOKEN:0:30}..."
echo

# 4. Test unauthorized dataset creation
echo "4. Testing Unauthorized Dataset Creation"
curl -s -X POST $BASE_URL/api/datasets/ \
  -H "Content-Type: application/json" \
  -d '{"name": "unauthorized", "description": "Should fail"}' | python3 -m json.tool
echo

# 5. Create dataset with auth
echo "5. Testing Authorized Dataset Creation"
curl -s -X POST $BASE_URL/api/datasets/ \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "api_test_dataset_'$(date +%s)'", "description": "Dataset created via API test"}' | python3 -m json.tool
echo

# 6. Grant permission
echo "6. Testing Permission Grant"
curl -s -X POST $BASE_URL/api/datasets/1/permissions \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"user_id": 2, "permission_type": "read"}' | python3 -m json.tool
echo

# 7. Test API docs
echo "7. Testing API Documentation"
curl -s -I $BASE_URL/docs | head -n 1
echo

echo "=== All Tests Complete ==="