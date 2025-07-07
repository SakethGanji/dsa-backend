#!/bin/bash

# E2E API Test Script for Sampling Functionality
# Tests all sampling methods and data retrieval

set -e  # Exit on error

# Configuration
BASE_URL="http://localhost:8000"
USERNAME="bg54677"
PASSWORD="string"
DATASET_ID=48
TABLE_KEY="Sales"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counter
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Function to print test results
print_test_result() {
    local test_name=$1
    local status=$2
    local details=$3
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    if [ "$status" = "PASS" ]; then
        echo -e "${GREEN}✓ $test_name${NC}"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        echo -e "${RED}✗ $test_name${NC}"
        echo -e "  ${YELLOW}Details: $details${NC}"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
}

# Function to get auth token
get_auth_token() {
    echo "Getting authentication token..." >&2
    local response=$(curl -s -X POST "$BASE_URL/api/auth/login" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "username=$USERNAME&password=$PASSWORD")
    
    local token=$(echo "$response" | grep -o '"access_token":"[^"]*' | sed 's/"access_token":"//')
    
    if [ -z "$token" ]; then
        echo "Failed to get auth token" >&2
        echo "$response" >&2
        exit 1
    fi
    
    echo "$token"
}

# Function to create sampling job
create_sampling_job() {
    local payload=$1
    local description=$2
    
    echo -e "\n${YELLOW}Testing: $description${NC}" >&2
    
    local response=$(curl -s -X POST "$BASE_URL/api/sampling/datasets/$DATASET_ID/jobs" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $TOKEN" \
        -d "$payload")
    
    local job_id=$(echo "$response" | grep -o '"job_id":"[^"]*' | sed 's/"job_id":"//')
    
    if [ -z "$job_id" ]; then
        print_test_result "$description - Job Creation" "FAIL" "Response: $response"
        echo ""
        return 1
    else
        print_test_result "$description - Job Creation" "PASS" ""
        echo "$job_id"
        return 0
    fi
}

# Function to wait for job completion
wait_for_job() {
    local job_id=$1
    local max_attempts=30
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        local response=$(curl -s -X GET "$BASE_URL/api/jobs/$job_id" \
            -H "Authorization: Bearer $TOKEN")
        
        local status=$(echo "$response" | grep -o '"status":"[^"]*' | sed 's/"status":"//')
        
        case "$status" in
            "completed")
                echo "completed"
                return 0
                ;;
            "failed")
                local error=$(echo "$response" | grep -o '"error_message":"[^"]*' | sed 's/"error_message":"//')
                echo "failed: $error"
                return 1
                ;;
            "pending"|"running")
                sleep 2
                attempt=$((attempt + 1))
                ;;
            *)
                echo "unknown status: $status"
                return 1
                ;;
        esac
    done
    
    echo "timeout"
    return 1
}

# Function to retrieve sampling data
retrieve_sampling_data() {
    local job_id=$1
    local table_key=${2:-$TABLE_KEY}
    local limit=${3:-10}
    
    local response=$(curl -s -X GET "$BASE_URL/api/sampling/jobs/$job_id/data?table_key=$table_key&limit=$limit" \
        -H "Authorization: Bearer $TOKEN")
    
    # Check if response contains error
    if echo "$response" | grep -q '"detail"'; then
        echo "ERROR: $response"
        return 1
    else
        echo "$response"
        return 0
    fi
}

# Function to validate data retrieval
validate_data_retrieval() {
    local job_id=$1
    local test_name=$2
    local expected_count=$3
    
    local data=$(retrieve_sampling_data "$job_id" "$TABLE_KEY" 100)
    
    if [[ "$data" == ERROR:* ]]; then
        print_test_result "$test_name - Data Retrieval" "FAIL" "$data"
        return 1
    fi
    
    # Count rows in response
    local row_count=$(echo "$data" | grep -o '"_logical_row_id"' | wc -l)
    
    if [ -n "$expected_count" ] && [ "$row_count" -ne "$expected_count" ]; then
        print_test_result "$test_name - Data Retrieval" "FAIL" "Expected $expected_count rows, got $row_count"
        return 1
    else
        print_test_result "$test_name - Data Retrieval" "PASS" "Retrieved $row_count rows"
        return 0
    fi
}

# Main test execution
echo "========================================="
echo "E2E Sampling API Test Suite"
echo "========================================="

# Get auth token
TOKEN=$(get_auth_token)
echo "Authentication successful"

# Test 1: Random Sampling (Unseeded)
echo -e "\n=== Test 1: Random Sampling (Unseeded) ==="
JOB_ID=$(create_sampling_job '{
    "source_ref": "main",
    "table_key": "Sales",
    "rounds": [{
        "round_number": 1,
        "method": "random",
        "parameters": {
            "sample_size": 10
        }
    }]
}' "Random Sampling (Unseeded)")

if [ -n "$JOB_ID" ]; then
    STATUS=$(wait_for_job "$JOB_ID")
    if [[ "$STATUS" == "completed" ]]; then
        print_test_result "Random Sampling (Unseeded) - Job Completion" "PASS" ""
        validate_data_retrieval "$JOB_ID" "Random Sampling (Unseeded)" 10
    else
        print_test_result "Random Sampling (Unseeded) - Job Completion" "FAIL" "$STATUS"
    fi
fi

# Test 2: Random Sampling (Seeded)
echo -e "\n=== Test 2: Random Sampling (Seeded) ==="
JOB_ID=$(create_sampling_job '{
    "source_ref": "main",
    "table_key": "Sales",
    "rounds": [{
        "round_number": 1,
        "method": "random",
        "parameters": {
            "sample_size": 15,
            "seed": 42
        }
    }]
}' "Random Sampling (Seeded)")

if [ -n "$JOB_ID" ]; then
    STATUS=$(wait_for_job "$JOB_ID")
    if [[ "$STATUS" == "completed" ]]; then
        print_test_result "Random Sampling (Seeded) - Job Completion" "PASS" ""
        validate_data_retrieval "$JOB_ID" "Random Sampling (Seeded)" 15
    else
        print_test_result "Random Sampling (Seeded) - Job Completion" "FAIL" "$STATUS"
    fi
fi

# Test 3: Systematic Sampling
echo -e "\n=== Test 3: Systematic Sampling ==="
JOB_ID=$(create_sampling_job '{
    "source_ref": "main",
    "table_key": "Sales",
    "rounds": [{
        "round_number": 1,
        "method": "systematic",
        "parameters": {
            "interval": 5,
            "start": 2
        }
    }]
}' "Systematic Sampling")

if [ -n "$JOB_ID" ]; then
    STATUS=$(wait_for_job "$JOB_ID")
    if [[ "$STATUS" == "completed" ]]; then
        print_test_result "Systematic Sampling - Job Completion" "PASS" ""
        validate_data_retrieval "$JOB_ID" "Systematic Sampling" ""
    else
        print_test_result "Systematic Sampling - Job Completion" "FAIL" "$STATUS"
    fi
fi

# Test 4: Stratified Sampling
echo -e "\n=== Test 4: Stratified Sampling ==="
JOB_ID=$(create_sampling_job '{
    "source_ref": "main",
    "table_key": "Sales",
    "rounds": [{
        "round_number": 1,
        "method": "stratified",
        "parameters": {
            "sample_size": 20,
            "strata_columns": ["region"],
            "min_per_stratum": 3,
            "seed": 123
        }
    }]
}' "Stratified Sampling")

if [ -n "$JOB_ID" ]; then
    STATUS=$(wait_for_job "$JOB_ID")
    if [[ "$STATUS" == "completed" ]]; then
        print_test_result "Stratified Sampling - Job Completion" "PASS" ""
        validate_data_retrieval "$JOB_ID" "Stratified Sampling" ""
    else
        print_test_result "Stratified Sampling - Job Completion" "FAIL" "$STATUS"
    fi
fi

# Test 5: Cluster Sampling
echo -e "\n=== Test 5: Cluster Sampling ==="
JOB_ID=$(create_sampling_job '{
    "source_ref": "main",
    "table_key": "Sales",
    "rounds": [{
        "round_number": 1,
        "method": "cluster",
        "parameters": {
            "cluster_column": "product_id",
            "num_clusters": 3,
            "samples_per_cluster": 5,
            "seed": 456
        }
    }]
}' "Cluster Sampling")

if [ -n "$JOB_ID" ]; then
    STATUS=$(wait_for_job "$JOB_ID")
    if [[ "$STATUS" == "completed" ]]; then
        print_test_result "Cluster Sampling - Job Completion" "PASS" ""
        validate_data_retrieval "$JOB_ID" "Cluster Sampling" ""
    else
        print_test_result "Cluster Sampling - Job Completion" "FAIL" "$STATUS"
    fi
fi

# Test 6: Multi-round Sampling
echo -e "\n=== Test 6: Multi-round Sampling ==="
JOB_ID=$(create_sampling_job '{
    "source_ref": "main",
    "table_key": "Sales",
    "rounds": [
        {
            "round_number": 1,
            "method": "random",
            "parameters": {
                "sample_size": 10,
                "seed": 100
            }
        },
        {
            "round_number": 2,
            "method": "systematic",
            "parameters": {
                "interval": 7,
                "start": 3
            }
        }
    ]
}' "Multi-round Sampling")

if [ -n "$JOB_ID" ]; then
    STATUS=$(wait_for_job "$JOB_ID")
    if [[ "$STATUS" == "completed" ]]; then
        print_test_result "Multi-round Sampling - Job Completion" "PASS" ""
        validate_data_retrieval "$JOB_ID" "Multi-round Sampling" ""
    else
        print_test_result "Multi-round Sampling - Job Completion" "FAIL" "$STATUS"
    fi
fi

# Test 7: Invalid Parameters (should fail)
echo -e "\n=== Test 7: Invalid Parameters Test ==="
RESPONSE=$(curl -s -X POST "$BASE_URL/api/sampling/datasets/$DATASET_ID/jobs" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TOKEN" \
    -d '{
        "source_ref": "main",
        "table_key": "Sales",
        "rounds": [{
            "round_number": 1,
            "method": "random",
            "parameters": {}
        }]
    }')

if echo "$RESPONSE" | grep -q '"detail"'; then
    print_test_result "Invalid Parameters - Validation Error" "PASS" "Correctly rejected empty parameters"
else
    print_test_result "Invalid Parameters - Validation Error" "FAIL" "Should have rejected empty parameters"
fi

# Test 8: Missing Required Fields
echo -e "\n=== Test 8: Missing Required Fields Test ==="
RESPONSE=$(curl -s -X POST "$BASE_URL/api/sampling/datasets/$DATASET_ID/jobs" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TOKEN" \
    -d '{
        "source_ref": "main",
        "table_key": "Sales",
        "rounds": [{
            "round_number": 1,
            "method": "stratified",
            "parameters": {
                "sample_size": 20
            }
        }]
    }')

if echo "$RESPONSE" | grep -q '"detail"'; then
    print_test_result "Missing Required Fields - Validation Error" "PASS" "Correctly rejected missing strata_columns"
else
    print_test_result "Missing Required Fields - Validation Error" "FAIL" "Should have rejected missing strata_columns"
fi

# Test 9: Wrong Table Key in Data Retrieval
echo -e "\n=== Test 9: Wrong Table Key Test ==="
# First create a valid job
JOB_ID=$(create_sampling_job '{
    "source_ref": "main",
    "table_key": "Sales",
    "rounds": [{
        "round_number": 1,
        "method": "random",
        "parameters": {
            "sample_size": 5
        }
    }]
}' "Wrong Table Key Test Setup")

if [ -n "$JOB_ID" ]; then
    STATUS=$(wait_for_job "$JOB_ID")
    if [[ "$STATUS" == "completed" ]]; then
        # Try to retrieve with wrong table key
        DATA=$(retrieve_sampling_data "$JOB_ID" "primary" 10)
        if [[ "$DATA" == ERROR:* ]]; then
            print_test_result "Wrong Table Key - Error Handling" "PASS" "Correctly rejected wrong table key"
        else
            print_test_result "Wrong Table Key - Error Handling" "FAIL" "Should have rejected wrong table key"
        fi
    fi
fi

# Test Summary
echo -e "\n========================================="
echo "Test Summary"
echo "========================================="
echo -e "Total Tests: $TOTAL_TESTS"
echo -e "${GREEN}Passed: $PASSED_TESTS${NC}"
echo -e "${RED}Failed: $FAILED_TESTS${NC}"

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "\n${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "\n${RED}Some tests failed!${NC}"
    exit 1
fi