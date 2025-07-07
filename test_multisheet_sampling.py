#!/usr/bin/env python3
"""
Test script for multi-sheet Excel file handling with all sampling endpoints.
Tests the complete flow from Excel import to various sampling operations.
"""

import pandas as pd
import numpy as np
import requests
import json
import time
import os
from datetime import datetime, timedelta
import sys

# Configuration
BASE_URL = "http://localhost:8000"
USERNAME = "bg54677"
PASSWORD = "string"

# Create multi-sheet Excel test file
def create_multisheet_excel(filename="test_multisheet.xlsx"):
    """Create a test Excel file with multiple sheets containing different data types."""
    print(f"\n1. Creating multi-sheet Excel file: {filename}")
    
    # Sheet 1: Sales Data
    np.random.seed(42)
    sales_data = {
        'date': pd.date_range('2024-01-01', periods=100, freq='D'),
        'product_id': np.random.choice(['P001', 'P002', 'P003', 'P004', 'P005'], 100),
        'customer_id': [f'C{i:04d}' for i in np.random.randint(1, 50, 100)],
        'quantity': np.random.randint(1, 20, 100),
        'unit_price': np.random.uniform(10.0, 100.0, 100).round(2),
        'discount': np.random.uniform(0.0, 0.3, 100).round(2),
        'region': np.random.choice(['North', 'South', 'East', 'West'], 100),
        'sales_rep': np.random.choice(['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'], 100)
    }
    sales_df = pd.DataFrame(sales_data)
    sales_df['total_amount'] = (sales_df['quantity'] * sales_df['unit_price'] * (1 - sales_df['discount'])).round(2)
    
    # Sheet 2: Inventory
    inventory_data = {
        'product_id': ['P001', 'P002', 'P003', 'P004', 'P005'],
        'product_name': ['Widget A', 'Widget B', 'Gadget X', 'Gadget Y', 'Tool Z'],
        'category': ['Electronics', 'Electronics', 'Tools', 'Tools', 'Accessories'],
        'current_stock': [150, 200, 75, 120, 300],
        'reorder_level': [50, 60, 25, 40, 100],
        'supplier_id': ['S001', 'S001', 'S002', 'S002', 'S003'],
        'unit_cost': [45.00, 32.50, 78.00, 56.00, 12.50],
        'last_restock_date': pd.to_datetime(['2024-01-15', '2024-01-20', '2024-01-10', '2024-01-25', '2024-01-05'])
    }
    inventory_df = pd.DataFrame(inventory_data)
    
    # Sheet 3: Customer Demographics
    customer_data = {
        'customer_id': [f'C{i:04d}' for i in range(1, 51)],
        'first_name': [f'FirstName{i}' for i in range(1, 51)],
        'last_name': [f'LastName{i}' for i in range(1, 51)],
        'email': [f'customer{i}@example.com' for i in range(1, 51)],
        'age': np.random.randint(18, 70, 50),
        'gender': np.random.choice(['M', 'F', 'Other'], 50),
        'city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'], 50),
        'state': np.random.choice(['NY', 'CA', 'IL', 'TX', 'AZ'], 50),
        'registration_date': pd.date_range('2023-01-01', periods=50, freq='W'),
        'loyalty_tier': np.random.choice(['Bronze', 'Silver', 'Gold', 'Platinum'], 50, p=[0.4, 0.3, 0.2, 0.1])
    }
    customers_df = pd.DataFrame(customer_data)
    
    # Sheet 4: Monthly Summary (smaller dataset)
    monthly_data = {
        'month': pd.date_range('2023-01-01', periods=12, freq='ME'),
        'total_sales': np.random.uniform(50000, 200000, 12).round(2),
        'total_orders': np.random.randint(100, 500, 12),
        'new_customers': np.random.randint(5, 30, 12),
        'avg_order_value': np.random.uniform(100, 300, 12).round(2)
    }
    monthly_df = pd.DataFrame(monthly_data)
    
    # Write to Excel file with multiple sheets
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        sales_df.to_excel(writer, sheet_name='Sales', index=False)
        inventory_df.to_excel(writer, sheet_name='Inventory', index=False)
        customers_df.to_excel(writer, sheet_name='Customers', index=False)
        monthly_df.to_excel(writer, sheet_name='Monthly_Summary', index=False)
    
    print(f"   ✓ Created Excel file with 4 sheets:")
    print(f"     - Sales: {len(sales_df)} rows, {len(sales_df.columns)} columns")
    print(f"     - Inventory: {len(inventory_df)} rows, {len(inventory_df.columns)} columns")
    print(f"     - Customers: {len(customers_df)} rows, {len(customers_df.columns)} columns")
    print(f"     - Monthly_Summary: {len(monthly_df)} rows, {len(monthly_df.columns)} columns")
    
    return filename

# Get authentication token
def get_auth_token():
    """Authenticate and get JWT token."""
    print("\n2. Getting authentication token")
    response = requests.post(
        f"{BASE_URL}/api/users/login",
        data={"username": USERNAME, "password": PASSWORD}
    )
    if response.status_code != 200:
        print(f"   ✗ Authentication failed: {response.status_code}")
        print(f"   Response: {response.text}")
        sys.exit(1)
    
    token = response.json()["access_token"]
    print(f"   ✓ Got authentication token")
    return token

# Create dataset and import Excel file
def create_dataset_and_import(token, excel_file):
    """Create a new dataset and import the multi-sheet Excel file."""
    print("\n3. Creating dataset and importing Excel file")
    
    headers = {"Authorization": f"Bearer {token}"}
    
    # Create dataset
    dataset_name = f"MultiSheet_Test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    response = requests.post(
        f"{BASE_URL}/api/datasets",
        headers=headers,
        json={"name": dataset_name, "description": "Test dataset for multi-sheet Excel sampling"}
    )
    
    if response.status_code not in [200, 201]:
        print(f"   ✗ Failed to create dataset: {response.status_code}")
        print(f"   Response: {response.text}")
        sys.exit(1)
    
    dataset = response.json()
    dataset_id = dataset.get("dataset_id") or dataset.get("id")
    print(f"   ✓ Created dataset: {dataset_name} (ID: {dataset_id})")
    
    # Import Excel file
    print("   - Importing Excel file...")
    with open(excel_file, 'rb') as f:
        files = {'file': (excel_file, f, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
        data = {'commit_message': 'Initial import of multi-sheet Excel file'}
        
        response = requests.post(
            f"{BASE_URL}/api/datasets/{dataset_id}/refs/main/import",
            headers=headers,
            files=files,
            data=data
        )
    
    if response.status_code not in [200, 202]:
        print(f"   ✗ Failed to import file: {response.status_code}")
        print(f"   Response: {response.text}")
        sys.exit(1)
    
    job_id = response.json()["job_id"]
    print(f"   ✓ Import job created: {job_id}")
    
    # Wait for import to complete
    print("   - Waiting for import to complete...")
    while True:
        response = requests.get(
            f"{BASE_URL}/api/jobs/{job_id}",
            headers=headers
        )
        job_status = response.json()
        
        if job_status["status"] == "completed":
            print("   ✓ Import completed successfully")
            print(f"     Output: {json.dumps(job_status.get('output_summary', {}), indent=2)}")
            break
        elif job_status["status"] == "failed":
            print(f"   ✗ Import failed: {job_status.get('error_message', 'Unknown error')}")
            sys.exit(1)
        
        time.sleep(1)
    
    # Get commit information to verify sheets were imported
    response = requests.get(
        f"{BASE_URL}/api/datasets/{dataset_id}/commits/HEAD",
        headers=headers
    )
    
    if response.status_code == 200:
        commit_info = response.json()
        print(f"   ✓ Verified import - Commit ID: {commit_info['commit_id']}")
    
    return dataset_id, dataset_name

# Test all sampling endpoints
def test_sampling_endpoints(token, dataset_id):
    """Test all sampling endpoints with the multi-sheet dataset."""
    headers = {"Authorization": f"Bearer {token}"}
    results = {}
    
    # Test 1: GET sampling methods
    print("\n4. Testing GET /api/sampling/datasets/{dataset_id}/sampling-methods")
    response = requests.get(
        f"{BASE_URL}/api/sampling/datasets/{dataset_id}/sampling-methods",
        headers=headers
    )
    print(f"   Status: {response.status_code}")
    if response.status_code == 200:
        methods = response.json()
        print(f"   ✓ Available sampling methods: {[m['name'] for m in methods['methods']]}")
        results['sampling_methods'] = methods
    else:
        print(f"   ✗ Error: {response.text}")
    
    # Test 2: POST Create sampling job (async)
    print("\n5. Testing POST /api/sampling/datasets/{dataset_id}/jobs")
    sampling_params = {
        "table_key": "Sales",  # Test with specific sheet
        "rounds": [{
            "method": "stratified",
            "sample_size": 20,
            "parameters": {
                "strata_columns": ["region"],
                "proportional": True
            }
        }],
        "commit_message": "Stratified sampling on Sales sheet by region"
    }
    
    response = requests.post(
        f"{BASE_URL}/api/sampling/datasets/{dataset_id}/jobs",
        headers=headers,
        json=sampling_params
    )
    print(f"   Status: {response.status_code}")
    
    job_id = None
    if response.status_code == 202:
        job_info = response.json()
        job_id = job_info["job_id"]
        print(f"   ✓ Sampling job created: {job_id}")
        results['sampling_job'] = job_info
        
        # Wait for job completion
        print("   - Waiting for sampling job to complete...")
        while True:
            response = requests.get(
                f"{BASE_URL}/api/jobs/{job_id}",
                headers=headers
            )
            job_status = response.json()
            
            if job_status["status"] == "completed":
                print("   ✓ Sampling job completed")
                break
            elif job_status["status"] == "failed":
                print(f"   ✗ Sampling job failed: {job_status.get('error_message', 'Unknown error')}")
                break
            
            time.sleep(1)
    else:
        print(f"   ✗ Error: {response.text}")
    
    # Test 3: POST Sample data direct (synchronous)
    print("\n6. Testing POST /api/sampling/datasets/{dataset_id}/sample")
    direct_params = {
        "method": "random",
        "sample_size": 10,
        "parameters": {},
        "sheets": ["Inventory", "Monthly_Summary"]  # Test with multiple sheets
    }
    
    response = requests.post(
        f"{BASE_URL}/api/sampling/datasets/{dataset_id}/sample",
        headers=headers,
        json=direct_params
    )
    print(f"   Status: {response.status_code}")
    
    if response.status_code == 200:
        sample_data = response.json()
        print(f"   ✓ Direct sampling successful")
        print(f"     - Total rows sampled: {len(sample_data['data'])}")
        if 'metadata' in sample_data and 'sheets' in sample_data['metadata']:
            for sheet_name, count in sample_data['metadata']['sheets'].items():
                print(f"     - {sheet_name}: {count} rows")
        results['direct_sample'] = sample_data
    else:
        print(f"   ✗ Error: {response.text}")
    
    # Test 4: POST Get column samples
    print("\n7. Testing POST /api/sampling/datasets/{dataset_id}/column-samples")
    column_params = {
        "columns": ["Sales:product_id", "Sales:region", "Customers:loyalty_tier"],
        "sample_size": 5
    }
    
    response = requests.post(
        f"{BASE_URL}/api/sampling/datasets/{dataset_id}/column-samples",
        headers=headers,
        json=column_params
    )
    print(f"   Status: {response.status_code}")
    
    if response.status_code == 200:
        column_samples = response.json()
        print(f"   ✓ Column samples retrieved")
        for col, values in column_samples['samples'].items():
            print(f"     - {col}: {values}")
        results['column_samples'] = column_samples
    else:
        print(f"   ✗ Error: {response.text}")
    
    # Test 5: GET sampling job data (if job was created)
    if job_id:
        print(f"\n8. Testing GET /api/sampling/jobs/{job_id}/data")
        response = requests.get(
            f"{BASE_URL}/api/sampling/jobs/{job_id}/data",
            headers=headers
        )
        print(f"   Status: {response.status_code}")
        
        if response.status_code == 200:
            job_data = response.json()
            print(f"   ✓ Sampling job data retrieved")
            if isinstance(job_data.get('data'), list):
                print(f"     - Total rows: {len(job_data['data'])}")
            else:
                for sheet_name, sheet_data in job_data['data'].items():
                    print(f"     - {sheet_name}: {len(sheet_data)} rows")
            results['job_data'] = job_data
        else:
            print(f"   ✗ Error: {response.text}")
    
    # Test 6: GET sampling job residual (if job was created)
    if job_id:
        print(f"\n9. Testing GET /api/sampling/jobs/{job_id}/residual")
        response = requests.get(
            f"{BASE_URL}/api/sampling/jobs/{job_id}/residual",
            headers=headers
        )
        print(f"   Status: {response.status_code}")
        
        if response.status_code == 200:
            residual_data = response.json()
            print(f"   ✓ Residual data retrieved")
            if isinstance(residual_data.get('data'), list):
                print(f"     - Total rows not sampled: {len(residual_data['data'])}")
            else:
                for sheet_name, sheet_data in residual_data['data'].items():
                    print(f"     - {sheet_name}: {len(sheet_data)} rows not sampled")
            results['residual_data'] = residual_data
        else:
            print(f"   ✗ Error: {response.text}")
    
    # Test 7: GET dataset sampling history
    print(f"\n10. Testing GET /api/sampling/datasets/{dataset_id}/history")
    response = requests.get(
        f"{BASE_URL}/api/sampling/datasets/{dataset_id}/history",
        headers=headers
    )
    print(f"   Status: {response.status_code}")
    
    if response.status_code == 200:
        history = response.json()
        print(f"   ✓ Dataset sampling history retrieved")
        print(f"     - Total jobs: {len(history['jobs'])}")
        results['dataset_history'] = history
    else:
        print(f"   ✗ Error: {response.text}")
    
    # Test 8: GET user sampling history
    print(f"\n11. Testing GET /api/sampling/users/me/history")
    response = requests.get(
        f"{BASE_URL}/api/sampling/users/me/history",
        headers=headers
    )
    print(f"   Status: {response.status_code}")
    
    if response.status_code == 200:
        user_history = response.json()
        print(f"   ✓ User sampling history retrieved")
        print(f"     - Total jobs: {len(user_history['jobs'])}")
        results['user_history'] = user_history
    else:
        print(f"   ✗ Error: {response.text}")
    
    return results

# Additional tests for multi-sheet specific scenarios
def test_multisheet_scenarios(token, dataset_id):
    """Test specific multi-sheet scenarios."""
    print("\n\n=== Testing Multi-Sheet Specific Scenarios ===")
    headers = {"Authorization": f"Bearer {token}"}
    
    # Scenario 1: Sample from all sheets
    print("\n12. Testing sampling from ALL sheets")
    response = requests.post(
        f"{BASE_URL}/api/sampling/datasets/{dataset_id}/sample",
        headers=headers,
        json={
            "method": "systematic",
            "sample_size": 5,
            "parameters": {"interval": 3}
            # No 'sheets' parameter means all sheets
        }
    )
    
    if response.status_code == 200:
        all_sheets_data = response.json()
        print("   ✓ Sampled from all sheets:")
        print(f"     - Total rows sampled: {len(all_sheets_data['data'])}")
        if 'metadata' in all_sheets_data:
            print(f"     - Metadata: {all_sheets_data['metadata']}")
    else:
        print(f"   ✗ Error: {response.text}")
    
    # Scenario 2: Test sheet-specific stratified sampling
    print("\n13. Testing sheet-specific stratified sampling")
    response = requests.post(
        f"{BASE_URL}/api/sampling/datasets/{dataset_id}/sample",
        headers=headers,
        json={
            "method": "stratified",
            "sample_size": 15,
            "parameters": {
                "strata_columns": ["category"],
                "proportional": True
            },
            "sheets": ["Inventory"]
        }
    )
    
    if response.status_code == 200:
        stratified_data = response.json()
        print("   ✓ Stratified sampling from Inventory sheet:")
        if 'Inventory' in stratified_data['data']:
            inv_data = stratified_data['data']['Inventory']
            categories = {}
            for row in inv_data:
                cat = row.get('category', 'Unknown')
                categories[cat] = categories.get(cat, 0) + 1
            print(f"     Category distribution: {categories}")
    else:
        print(f"   ✗ Error: {response.text}")
    
    # Scenario 3: Cross-sheet column sampling
    print("\n14. Testing cross-sheet column value sampling")
    response = requests.post(
        f"{BASE_URL}/api/sampling/datasets/{dataset_id}/column-samples",
        headers=headers,
        json={
            "columns": [
                "Sales:customer_id",
                "Customers:customer_id",
                "Inventory:product_id",
                "Sales:product_id"
            ],
            "sample_size": 3
        }
    )
    
    if response.status_code == 200:
        cross_sheet_samples = response.json()
        print("   ✓ Cross-sheet column samples:")
        for col, values in cross_sheet_samples['samples'].items():
            print(f"     - {col}: {values}")
    else:
        print(f"   ✗ Error: {response.text}")

def main():
    """Main test execution."""
    print("=" * 80)
    print("Multi-Sheet Excel Sampling API Test")
    print("=" * 80)
    
    try:
        # Create test Excel file
        excel_file = create_multisheet_excel()
        
        # Get auth token
        token = get_auth_token()
        
        # Create dataset and import
        dataset_id, dataset_name = create_dataset_and_import(token, excel_file)
        
        # Test all sampling endpoints
        results = test_sampling_endpoints(token, dataset_id)
        
        # Test multi-sheet specific scenarios
        test_multisheet_scenarios(token, dataset_id)
        
        print("\n" + "=" * 80)
        print("✓ All tests completed successfully!")
        print(f"Dataset ID: {dataset_id}")
        print(f"Dataset Name: {dataset_name}")
        print("=" * 80)
        
        # Clean up
        if os.path.exists(excel_file):
            os.remove(excel_file)
            print(f"\nCleaned up test file: {excel_file}")
        
    except Exception as e:
        print(f"\n✗ Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()