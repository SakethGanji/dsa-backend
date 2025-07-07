#!/usr/bin/env python3
"""
Test script to verify sampling parameter validation
"""
import requests
import json

# Test cases for sampling validation
test_cases = [
    {
        "name": "Valid random sampling",
        "payload": {
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
        },
        "should_succeed": True
    },
    {
        "name": "Invalid random sampling - missing sample_size",
        "payload": {
            "source_ref": "main",
            "table_key": "primary",
            "rounds": [{
                "round_number": 1,
                "method": "random",
                "parameters": {}
            }]
        },
        "should_succeed": False,
        "expected_error": "Random sampling requires 'sample_size' parameter"
    },
    {
        "name": "Invalid random sampling - negative sample_size",
        "payload": {
            "source_ref": "main",
            "table_key": "primary",
            "rounds": [{
                "round_number": 1,
                "method": "random",
                "parameters": {
                    "sample_size": -10
                }
            }]
        },
        "should_succeed": False,
        "expected_error": "sample_size must be a positive integer"
    }
]

def test_sampling_validation(base_url="http://localhost:8000", dataset_id=48):
    """Test sampling parameter validation"""
    
    # You'll need a valid auth token
    auth_token = "YOUR_AUTH_TOKEN_HERE"
    
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    
    endpoint = f"{base_url}/api/sampling/datasets/{dataset_id}/jobs"
    
    for test in test_cases:
        print(f"\nTesting: {test['name']}")
        print(f"Payload: {json.dumps(test['payload'], indent=2)}")
        
        response = requests.post(endpoint, json=test['payload'], headers=headers)
        
        if test['should_succeed']:
            if response.status_code == 200:
                print("✓ Passed - Request succeeded as expected")
            else:
                print(f"✗ Failed - Expected success but got {response.status_code}")
                print(f"Response: {response.text}")
        else:
            if response.status_code == 422:  # Validation error
                error_detail = response.json()
                print(f"✓ Passed - Validation failed as expected")
                print(f"Error: {error_detail}")
            else:
                print(f"✗ Failed - Expected validation error but got {response.status_code}")
                print(f"Response: {response.text}")


if __name__ == "__main__":
    # Update these values
    test_sampling_validation(
        base_url="http://localhost:8000",
        dataset_id=48  # Use a valid dataset ID
    )