#!/bin/bash

# Configuration
API_BASE="http://localhost:8000"
TOKEN="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJiZzU0Njc3Iiwicm9sZV9pZCI6MSwicm9sZV9uYW1lIjoiYWRtaW4iLCJleHAiOjE3NTE4NjI5ODZ9.7wZaMMLpH7n9mE2vp2QI01cxCRHEEd-f5UW5wxBiP_M"
DATASET_ID=48
DB_URL="postgresql://postgres:postgres@localhost:5432/postgres"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Function to check job status
check_job_status() {
    local job_id=$1
    local max_attempts=60
    local attempt=0
    
    echo "Checking job status for $job_id..."
    
    while [ $attempt -lt $max_attempts ]; do
        response=$(curl -s "${API_BASE}/api/jobs/${job_id}" \
            -H "Authorization: Bearer ${TOKEN}")
        
        status=$(echo "$response" | jq -r '.status')
        
        if [ "$status" = "completed" ]; then
            echo -e "${GREEN}✓ Job completed successfully${NC}"
            echo "$response" | jq -c '{id, status, total_sampled: .output_summary.total_sampled, duration: .duration_seconds}'
            return 0
        elif [ "$status" = "failed" ]; then
            echo -e "${RED}✗ Job failed${NC}"
            error=$(echo "$response" | jq -r '.error_message')
            echo "Error: $error"
            return 1
        fi
        
        sleep 2
        ((attempt++))
    done
    
    echo -e "${RED}✗ Job timed out${NC}"
    return 1
}

# Function to retrieve and verify data
check_job_data() {
    local job_id=$1
    local table_key=$2
    
    echo "Retrieving data for job $job_id (table: $table_key)..."
    
    response=$(curl -s "${API_BASE}/api/sampling/jobs/${job_id}/data?table_key=${table_key}&limit=5" \
        -H "Authorization: Bearer ${TOKEN}")
    
    if echo "$response" | jq -e '.data' > /dev/null 2>&1; then
        row_count=$(echo "$response" | jq '.data | length')
        total_rows=$(echo "$response" | jq '.pagination.total')
        echo -e "${GREEN}✓ Data retrieved: ${row_count} rows (total: ${total_rows})${NC}"
        echo "Sample data:"
        echo "$response" | jq '.data[0]' | head -10
        return 0
    else
        echo -e "${RED}✗ Failed to retrieve data${NC}"
        echo "$response" | jq '.'
        return 1
    fi
}

# Test function
test_sampling_method() {
    local test_name=$1
    local payload=$2
    local table_key=${3:-"primary"}
    
    echo ""
    echo -e "${YELLOW}=== Testing: $test_name ===${NC}"
    echo "Payload:"
    echo "$payload" | jq '.'
    
    # Create job
    response=$(curl -s -X POST "${API_BASE}/api/sampling/datasets/${DATASET_ID}/jobs" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d "$payload")
    
    job_id=$(echo "$response" | jq -r '.job_id')
    
    if [ "$job_id" = "null" ] || [ -z "$job_id" ]; then
        echo -e "${RED}✗ Failed to create job${NC}"
        echo "$response" | jq '.'
        return 1
    fi
    
    echo "Job created: $job_id"
    
    # Check status
    if check_job_status "$job_id"; then
        # Retrieve data
        check_job_data "$job_id" "$table_key"
    fi
}

echo "Starting comprehensive sampling tests..."
echo "Dataset ID: $DATASET_ID"
echo ""

# Test 1: Random sampling without seed
test_sampling_method "Random Sampling (no seed)" '{
    "source_ref": "main",
    "table_key": "primary",
    "rounds": [{
        "round_number": 1,
        "method": "random",
        "parameters": {
            "sample_size": 100
        }
    }]
}'

# Test 2: Random sampling with seed
test_sampling_method "Random Sampling (with seed)" '{
    "source_ref": "main",
    "table_key": "primary",
    "rounds": [{
        "round_number": 1,
        "method": "random",
        "parameters": {
            "sample_size": 100,
            "seed": 42
        }
    }]
}'

# Test 3: Systematic sampling
test_sampling_method "Systematic Sampling" '{
    "source_ref": "main",
    "table_key": "primary",
    "rounds": [{
        "round_number": 1,
        "method": "systematic",
        "parameters": {
            "interval": 10,
            "start": 5
        }
    }]
}'

# Test 4: Stratified sampling (need to check available columns first)
echo ""
echo "Checking available columns for stratified sampling..."
# First, let's get some data to see what columns are available
sample_response=$(curl -s "${API_BASE}/api/datasets/${DATASET_ID}/refs/main/data?table_key=primary&limit=1" \
    -H "Authorization: Bearer ${TOKEN}")

if echo "$sample_response" | jq -e '.data[0]' > /dev/null 2>&1; then
    echo "Available columns:"
    echo "$sample_response" | jq '.data[0] | keys'
    
    # Assuming there's a category or region column - adjust based on actual data
    test_sampling_method "Stratified Sampling" '{
        "source_ref": "main",
        "table_key": "primary",
        "rounds": [{
            "round_number": 1,
            "method": "stratified",
            "parameters": {
                "sample_size": 100,
                "strata_columns": ["category"],
                "min_per_stratum": 5,
                "proportional": true
            }
        }]
    }'
fi

# Test 5: Multi-round sampling
test_sampling_method "Multi-round Sampling" '{
    "source_ref": "main",
    "table_key": "primary",
    "rounds": [
        {
            "round_number": 1,
            "method": "random",
            "parameters": {
                "sample_size": 50,
                "seed": 123
            },
            "output_name": "First Random Sample"
        },
        {
            "round_number": 2,
            "method": "systematic",
            "parameters": {
                "interval": 20
            },
            "output_name": "Systematic Sample"
        }
    ],
    "export_residual": true,
    "residual_output_name": "Remaining Data"
}'

# Test 6: Sampling with filters
test_sampling_method "Random Sampling with Filters" '{
    "source_ref": "main",
    "table_key": "primary",
    "rounds": [{
        "round_number": 1,
        "method": "random",
        "parameters": {
            "sample_size": 50,
            "filters": {
                "conditions": [{
                    "column": "price",
                    "operator": ">",
                    "value": 100
                }],
                "logic": "AND"
            }
        }
    }]
}'

# Test 7: Sampling from non-primary table
test_sampling_method "Sampling from Sales table" '{
    "source_ref": "main",
    "table_key": "Sales",
    "rounds": [{
        "round_number": 1,
        "method": "random",
        "parameters": {
            "sample_size": 50
        }
    }]
}' "Sales"

# Test 8: Edge cases
echo ""
echo -e "${YELLOW}=== Testing Edge Cases ===${NC}"

# Very small sample
test_sampling_method "Very small sample (1 row)" '{
    "source_ref": "main",
    "table_key": "primary",
    "rounds": [{
        "round_number": 1,
        "method": "random",
        "parameters": {
            "sample_size": 1
        }
    }]
}'

# Large sample
test_sampling_method "Large sample (10000 rows)" '{
    "source_ref": "main",
    "table_key": "primary",
    "rounds": [{
        "round_number": 1,
        "method": "random",
        "parameters": {
            "sample_size": 10000,
            "seed": 999
        }
    }]
}'

echo ""
echo "All tests completed!"