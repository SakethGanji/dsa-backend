#!/usr/bin/env python3
"""Final test for sql-transform endpoint with server-side processing."""

import requests
import time
import json

BASE_URL = "http://localhost:8000"

# 1. Get authentication token
print("1. Getting authentication token...")
token_response = requests.post(
    f"{BASE_URL}/api/users/token",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    data={"username": "bg54677", "password": "password"}
)

if token_response.status_code != 200:
    print(f"Failed to get token: {token_response.text}")
    exit(1)

token = token_response.json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}
print(f"Token obtained: {token[:30]}...")

# 2. Test preview mode
print("\n2. Testing PREVIEW mode (save=false)...")
preview_request = {
    "sources": [{
        "alias": "s",
        "dataset_id": 1,
        "ref": "main",
        "table_key": "default"
    }],
    "sql": "SELECT (data->>'id')::text as id, data->>'product' as product FROM s",
    "save": False,
    "limit": 5
}

preview_response = requests.post(
    f"{BASE_URL}/api/workbench/sql-transform",
    headers={**headers, "Content-Type": "application/json"},
    json=preview_request
)

print(f"Preview response status: {preview_response.status_code}")
if preview_response.status_code == 200:
    result = preview_response.json()
    print(f"Rows returned: {result.get('row_count', 0)}")
    if result.get('data'):
        print("Sample data:")
        for row in result['data'][:3]:
            print(f"  {row}")
else:
    print(f"Error: {preview_response.text}")

# 3. Test save mode
print("\n3. Testing SAVE mode (save=true)...")
save_request = {
    "sources": [{
        "alias": "s",
        "dataset_id": 1,
        "ref": "main",
        "table_key": "default"
    }],
    "sql": """
        SELECT 
            (data->>'id')::text as product_id,
            data->>'product' as product_name,
            ((data->>'price')::numeric * (data->>'quantity')::numeric)::text as total_value
        FROM s
    """,
    "save": True,
    "target": {
        "dataset_id": 1,
        "ref": "main",
        "table_key": "product_totals",
        "message": "Server-side transformation test"
    }
}

transform_response = requests.post(
    f"{BASE_URL}/api/workbench/sql-transform",
    headers={**headers, "Content-Type": "application/json"},
    json=save_request
)

print(f"Transform response status: {transform_response.status_code}")
if transform_response.status_code != 200:
    print(f"Error: {transform_response.text}")
    exit(1)

job_info = transform_response.json()
job_id = job_info.get("job_id")
print(f"Job created: {job_id}")

# 4. Poll job status
print("\n4. Checking job status...")
max_attempts = 10
for i in range(max_attempts):
    time.sleep(2)
    
    job_response = requests.get(
        f"{BASE_URL}/api/jobs/{job_id}",
        headers=headers
    )
    
    if job_response.status_code != 200:
        print(f"Failed to get job status: {job_response.text}")
        break
    
    job_data = job_response.json()
    status = job_data.get("status")
    print(f"Attempt {i+1}: Status = {status}")
    
    if status == "completed":
        print("\n✓ Job completed successfully!")
        
        # Verify in database
        import subprocess
        result = subprocess.run([
            "psql", "-h", "localhost", "-U", "postgres", "-d", "postgres",
            "-c", f"""
                SELECT 'Rows in new commit:' as info, COUNT(*) as count
                FROM dsa_core.commits c
                JOIN dsa_core.commit_rows cr ON c.commit_id = cr.commit_id
                WHERE c.message = 'Server-side transformation test'
                AND cr.logical_row_id LIKE 'product_totals:%';
            """
        ], env={"PGPASSWORD": "postgres"}, capture_output=True, text=True)
        
        print("\nDatabase verification:")
        print(result.stdout)
        break
        
    elif status == "failed":
        print(f"\n✗ Job failed!")
        print(f"Error: {job_data.get('error_message', 'Unknown error')}")
        break

print("\nTest complete!")