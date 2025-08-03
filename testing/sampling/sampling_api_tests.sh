#!/bin/bash
# Comprehensive sampling API test suite

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Configuration
BASE_URL="http://localhost:8001"
TOKEN=$(cat ~/Projects/dsa/token.txt)

# Helper function to make API calls
api_call() {
    local method=$1
    local endpoint=$2
    local data=$3
    
    if [ "$method" = "GET" ]; then
        curl -s -X GET "${BASE_URL}${endpoint}" \
            -H "Authorization: Bearer $TOKEN"
    else
        curl -s -X POST "${BASE_URL}${endpoint}" \
            -H "Authorization: Bearer $TOKEN" \
            -H "Content-Type: application/json" \
            -d "$data"
    fi
}

# Test 1: Basic random sampling
test_random_sampling() {
    echo "Testing random sampling..."
    
    local response=$(api_call POST "/api/sampling/datasets/1/jobs" '{
        "source_dataset_id": 1,
        "output_name": "test_random",
        "rounds": [{
            "round_number": 1,
            "method": "random",
            "parameters": {"sample_size": 10, "seed": 42}
        }]
    }')
    
    if echo "$response" | grep -q "job_id"; then
        echo -e "${GREEN}✓ Random sampling test passed${NC}"
        return 0
    else
        echo -e "${RED}✗ Random sampling test failed${NC}"
        echo "$response"
        return 1
    fi
}

# Test 2: Filtered sampling
test_filtered_sampling() {
    echo "Testing filtered sampling..."
    
    local response=$(api_call POST "/api/sampling/datasets/1/jobs" '{
        "source_dataset_id": 1,
        "output_name": "test_filtered",
        "rounds": [{
            "round_number": 1,
            "method": "random",
            "parameters": {"sample_size": 10},
            "filters": {
                "expression": "model_year > 2020 AND county = '\''King'\''"
            }
        }]
    }')
    
    if echo "$response" | grep -q "job_id"; then
        echo -e "${GREEN}✓ Filtered sampling test passed${NC}"
        return 0
    else
        echo -e "${RED}✗ Filtered sampling test failed${NC}"
        echo "$response"
        return 1
    fi
}

# Test 3: Invalid method validation
test_invalid_method() {
    echo "Testing invalid method validation..."
    
    local response=$(api_call POST "/api/sampling/datasets/1/jobs" '{
        "source_dataset_id": 1,
        "output_name": "test_invalid",
        "rounds": [{
            "round_number": 1,
            "method": "invalid_method",
            "parameters": {"sample_size": 10}
        }]
    }')
    
    if echo "$response" | grep -q "400\|Invalid sampling method"; then
        echo -e "${GREEN}✓ Invalid method validation test passed${NC}"
        return 0
    else
        echo -e "${RED}✗ Invalid method validation test failed${NC}"
        echo "$response"
        return 1
    fi
}

# Test 4: Residual export
test_residual_export() {
    echo "Testing residual export..."
    
    local response=$(api_call POST "/api/sampling/datasets/1/jobs" '{
        "source_dataset_id": 1,
        "output_name": "test_residual",
        "export_residual": true,
        "rounds": [{
            "round_number": 1,
            "method": "random",
            "parameters": {"sample_size": 50}
        }]
    }')
    
    if echo "$response" | grep -q "job_id"; then
        local job_id=$(echo "$response" | jq -r '.job_id')
        echo "  Job created: $job_id"
        
        # Wait for job completion
        sleep 5
        
        # Check if residual branch exists
        local branch_check=$(api_call GET "/api/datasets/1/refs/smpl-test_residual_residual/tables")
        if echo "$branch_check" | grep -q "residual"; then
            echo -e "${GREEN}✓ Residual export test passed${NC}"
            return 0
        else
            echo -e "${RED}✗ Residual branch not found${NC}"
            return 1
        fi
    else
        echo -e "${RED}✗ Residual export test failed${NC}"
        echo "$response"
        return 1
    fi
}

# Run all tests
echo "=== Sampling API Test Suite ==="
echo

test_random_sampling
test_filtered_sampling
test_invalid_method
test_residual_export

echo
echo "=== Test Suite Complete ==="