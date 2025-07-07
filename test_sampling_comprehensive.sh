#!/bin/bash

# Configuration
API_BASE="http://localhost:8000"
TOKEN="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJiZzU0Njc3Iiwicm9sZV9pZCI6MSwicm9sZV9uYW1lIjoiYWRtaW4iLCJleHAiOjE3NTE4NjI5ODZ9.7wZaMMLpH7n9mE2vp2QI01cxCRHEEd-f5UW5wxBiP_M"
DATASET_ID=48

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Results tracking
PASSED=0
FAILED=0

# Function to test a sampling job
run_sampling_test() {
    local test_name=$1
    local payload=$2
    local table_key=${3:-"Sales"}
    local expected_behavior=${4:-"success"}
    
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}TEST: $test_name${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    # Create job
    echo "Creating sampling job..."
    response=$(curl -s -X POST "${API_BASE}/api/sampling/datasets/${DATASET_ID}/jobs" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${TOKEN}" \
        -d "$payload")
    
    # Check if job creation succeeded
    job_id=$(echo "$response" | jq -r '.job_id // empty')
    
    if [ -z "$job_id" ]; then
        if [ "$expected_behavior" = "fail_create" ]; then
            echo -e "${GREEN}✓ Job creation failed as expected${NC}"
            echo "Error: $(echo "$response" | jq -r '.detail')"
            ((PASSED++))
            return 0
        else
            echo -e "${RED}✗ Failed to create job${NC}"
            echo "$response" | jq '.'
            ((FAILED++))
            return 1
        fi
    fi
    
    echo "Job ID: $job_id"
    
    # Wait for job completion
    echo "Waiting for job to complete..."
    max_attempts=30
    attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        job_status=$(curl -s "${API_BASE}/api/jobs/${job_id}" \
            -H "Authorization: Bearer ${TOKEN}")
        
        status=$(echo "$job_status" | jq -r '.status')
        
        if [ "$status" = "completed" ]; then
            echo -e "${GREEN}✓ Job completed successfully${NC}"
            
            # Show job summary
            echo "$job_status" | jq '{
                duration: .duration_seconds,
                total_sampled: .output_summary.total_sampled,
                rounds: .output_summary.round_results
            }'
            
            # Retrieve and verify data
            echo ""
            echo "Retrieving sampled data..."
            data_response=$(curl -s "${API_BASE}/api/sampling/jobs/${job_id}/data?table_key=${table_key}&limit=5" \
                -H "Authorization: Bearer ${TOKEN}")
            
            if echo "$data_response" | jq -e '.data' > /dev/null 2>&1; then
                total_rows=$(echo "$data_response" | jq '.pagination.total')
                echo -e "${GREEN}✓ Data retrieved successfully (${total_rows} total rows)${NC}"
                echo "First row sample:"
                echo "$data_response" | jq '.data[0]'
                ((PASSED++))
                return 0
            else
                echo -e "${RED}✗ Failed to retrieve data${NC}"
                echo "$data_response" | jq '.'
                ((FAILED++))
                return 1
            fi
            
        elif [ "$status" = "failed" ]; then
            if [ "$expected_behavior" = "fail_execute" ]; then
                echo -e "${GREEN}✓ Job failed as expected${NC}"
                echo "Error: $(echo "$job_status" | jq -r '.error_message')"
                ((PASSED++))
                return 0
            else
                echo -e "${RED}✗ Job failed unexpectedly${NC}"
                echo "Error: $(echo "$job_status" | jq -r '.error_message')"
                ((FAILED++))
                return 1
            fi
        fi
        
        sleep 2
        ((attempt++))
    done
    
    echo -e "${RED}✗ Job timed out${NC}"
    ((FAILED++))
    return 1
}

# Start testing
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║          COMPREHENSIVE SAMPLING TESTS                       ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Test 1: Random sampling without seed
run_sampling_test "Random Sampling (unseeded)" '{
    "source_ref": "main",
    "table_key": "Sales",
    "rounds": [{
        "round_number": 1,
        "method": "random",
        "parameters": {
            "sample_size": 20
        }
    }]
}'

# Test 2: Random sampling with seed (reproducible)
run_sampling_test "Random Sampling (seeded)" '{
    "source_ref": "main",
    "table_key": "Sales",
    "rounds": [{
        "round_number": 1,
        "method": "random",
        "parameters": {
            "sample_size": 20,
            "seed": 42
        }
    }]
}'

# Test 3: Systematic sampling
run_sampling_test "Systematic Sampling" '{
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
}'

# Test 4: Stratified sampling by region
run_sampling_test "Stratified Sampling (by region)" '{
    "source_ref": "main",
    "table_key": "Sales",
    "rounds": [{
        "round_number": 1,
        "method": "stratified",
        "parameters": {
            "sample_size": 40,
            "strata_columns": ["region"],
            "min_per_stratum": 5,
            "proportional": true,
            "seed": 123
        }
    }]
}'

# Test 5: Cluster sampling by product_id
run_sampling_test "Cluster Sampling" '{
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
}'

# Test 6: Multi-round sampling (exclusion test)
run_sampling_test "Multi-round Sampling (with exclusion)" '{
    "source_ref": "main",
    "table_key": "Sales",
    "rounds": [
        {
            "round_number": 1,
            "method": "random",
            "parameters": {
                "sample_size": 10,
                "seed": 111
            },
            "output_name": "First Random Sample"
        },
        {
            "round_number": 2,
            "method": "random",
            "parameters": {
                "sample_size": 10,
                "seed": 222
            },
            "output_name": "Second Random Sample (excludes first)"
        },
        {
            "round_number": 3,
            "method": "systematic",
            "parameters": {
                "interval": 10
            },
            "output_name": "Systematic Sample (excludes previous)"
        }
    ]
}'

# Test 7: Sampling with filters
run_sampling_test "Filtered Random Sampling" '{
    "source_ref": "main",
    "table_key": "Sales",
    "rounds": [{
        "round_number": 1,
        "method": "random",
        "parameters": {
            "sample_size": 20,
            "seed": 789,
            "filters": {
                "conditions": [
                    {
                        "column": "region",
                        "operator": "in",
                        "value": ["North", "South"]
                    },
                    {
                        "column": "total_amount",
                        "operator": ">",
                        "value": 50
                    }
                ],
                "logic": "AND"
            }
        }
    }]
}'

# Test 8: Sampling with column selection
run_sampling_test "Sampling with Column Selection" '{
    "source_ref": "main",
    "table_key": "Sales",
    "rounds": [{
        "round_number": 1,
        "method": "random",
        "parameters": {
            "sample_size": 15,
            "seed": 333,
            "selection": {
                "columns": ["date", "region", "product_id", "total_amount"],
                "order_by": "total_amount",
                "order_desc": true
            }
        }
    }]
}'

# Test 9: Export residual data
run_sampling_test "Sampling with Residual Export" '{
    "source_ref": "main",
    "table_key": "Sales",
    "export_residual": true,
    "residual_output_name": "Unsampled Records",
    "rounds": [{
        "round_number": 1,
        "method": "random",
        "parameters": {
            "sample_size": 30,
            "seed": 999
        }
    }]
}'

# Test 10: Different table (Customers)
run_sampling_test "Sampling from Customers Table" '{
    "source_ref": "main",
    "table_key": "Customers",
    "rounds": [{
        "round_number": 1,
        "method": "random",
        "parameters": {
            "sample_size": 10
        }
    }]
}' "Customers"

# Test 11: Edge case - very small sample
run_sampling_test "Edge Case: Single Row Sample" '{
    "source_ref": "main",
    "table_key": "Sales",
    "rounds": [{
        "round_number": 1,
        "method": "random",
        "parameters": {
            "sample_size": 1
        }
    }]
}'

# Test 12: Edge case - sample larger than dataset
run_sampling_test "Edge Case: Sample Size > Dataset Size" '{
    "source_ref": "main",
    "table_key": "Sales",
    "rounds": [{
        "round_number": 1,
        "method": "random",
        "parameters": {
            "sample_size": 1000
        }
    }]
}'

# Test 13: Invalid parameters (should fail at API level)
run_sampling_test "Invalid: Missing sample_size" '{
    "source_ref": "main",
    "table_key": "Sales",
    "rounds": [{
        "round_number": 1,
        "method": "random",
        "parameters": {}
    }]
}' "Sales" "fail_create"

# Test 14: Complex multi-round with different methods
run_sampling_test "Complex Multi-method Sampling" '{
    "source_ref": "main",
    "table_key": "Sales",
    "commit_message": "Complex multi-method sampling test",
    "rounds": [
        {
            "round_number": 1,
            "method": "stratified",
            "parameters": {
                "sample_size": 20,
                "strata_columns": ["region"],
                "seed": 100
            },
            "output_name": "Stratified by Region"
        },
        {
            "round_number": 2,
            "method": "cluster",
            "parameters": {
                "cluster_column": "sales_rep",
                "num_clusters": 2,
                "seed": 200
            },
            "output_name": "Clustered by Sales Rep"
        },
        {
            "round_number": 3,
            "method": "random",
            "parameters": {
                "sample_size": 10
            },
            "output_name": "Final Random Sample"
        }
    ]
}'

# Print summary
echo ""
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                    TEST SUMMARY                             ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${RED}Failed: $FAILED${NC}"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed! 🎉${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed. Please check the logs above.${NC}"
    exit 1
fi