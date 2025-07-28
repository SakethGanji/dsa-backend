#!/usr/bin/env python3
"""
Example Python client for using the text-based filter API.
Demonstrates various filter expressions and use cases.
"""

import requests
import json
from typing import Dict, Any, List, Optional


class SamplingAPIClient:
    """Client for interacting with the sampling API."""
    
    def __init__(self, base_url: str, auth_token: str):
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {auth_token}"
        }
    
    def create_sampling_job(
        self,
        dataset_id: int,
        filter_expression: str,
        sample_size: int = 1000,
        method: str = "random",
        source_ref: str = "main",
        table_key: str = "primary",
        output_branch_name: Optional[str] = None,
        seed: Optional[int] = None,
        columns: Optional[List[str]] = None,
        order_by: Optional[str] = None,
        order_desc: bool = False
    ) -> Dict[str, Any]:
        """
        Create a sampling job with text-based filters.
        
        Args:
            dataset_id: ID of the dataset to sample from
            filter_expression: SQL-like filter expression
            sample_size: Number of records to sample
            method: Sampling method (random, stratified, cluster, systematic)
            source_ref: Source branch/ref name
            table_key: Table to sample from
            output_branch_name: Name for the output branch
            seed: Random seed for reproducibility
            columns: List of columns to include in output
            order_by: Column to order results by
            order_desc: Whether to order descending
            
        Returns:
            API response as dictionary
        """
        # Build the request payload
        payload = {
            "source_ref": source_ref,
            "table_key": table_key,
            "rounds": [{
                "round_number": 1,
                "method": method,
                "parameters": {
                    "sample_size": sample_size,
                    "filters": {
                        "expression": filter_expression
                    }
                }
            }]
        }
        
        # Add optional parameters
        if output_branch_name:
            payload["output_branch_name"] = output_branch_name
            
        if seed is not None:
            payload["rounds"][0]["parameters"]["seed"] = seed
            
        if columns or order_by:
            selection = {}
            if columns:
                selection["columns"] = columns
            if order_by:
                selection["order_by"] = order_by
                selection["order_desc"] = order_desc
            payload["rounds"][0]["parameters"]["selection"] = selection
        
        # Make the API call
        url = f"{self.base_url}/sampling/datasets/{dataset_id}/jobs"
        response = requests.post(url, json=payload, headers=self.headers)
        response.raise_for_status()
        
        return response.json()
    
    def create_stratified_sampling_job(
        self,
        dataset_id: int,
        filter_expression: str,
        strata_columns: List[str],
        sample_size: int = 1000,
        source_ref: str = "main",
        output_branch_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a stratified sampling job with filters."""
        payload = {
            "source_ref": source_ref,
            "table_key": "primary",
            "rounds": [{
                "round_number": 1,
                "method": "stratified",
                "parameters": {
                    "strata_columns": strata_columns,
                    "sample_size": sample_size,
                    "filters": {
                        "expression": filter_expression
                    }
                }
            }]
        }
        
        if output_branch_name:
            payload["output_branch_name"] = output_branch_name
        
        url = f"{self.base_url}/sampling/datasets/{dataset_id}/jobs"
        response = requests.post(url, json=payload, headers=self.headers)
        response.raise_for_status()
        
        return response.json()
    
    def create_multi_round_sampling(
        self,
        dataset_id: int,
        rounds: List[Dict[str, Any]],
        source_ref: str = "main",
        output_branch_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a multi-round sampling job with different filters per round."""
        payload = {
            "source_ref": source_ref,
            "table_key": "primary",
            "rounds": rounds
        }
        
        if output_branch_name:
            payload["output_branch_name"] = output_branch_name
        
        url = f"{self.base_url}/sampling/datasets/{dataset_id}/jobs"
        response = requests.post(url, json=payload, headers=self.headers)
        response.raise_for_status()
        
        return response.json()


# Example usage
if __name__ == "__main__":
    # Initialize client
    client = SamplingAPIClient(
        base_url="http://localhost:8000",
        auth_token="your-auth-token-here"
    )
    
    # Example 1: Simple filter
    print("Example 1: Simple age and status filter")
    result = client.create_sampling_job(
        dataset_id=1,
        filter_expression="age > 25 AND status = 'active'",
        sample_size=1000,
        output_branch_name="active_adults_sample"
    )
    print(f"Job ID: {result['job_id']}")
    print()
    
    # Example 2: Complex filter with IN operator
    print("Example 2: Department filter with IN operator")
    result = client.create_sampling_job(
        dataset_id=1,
        filter_expression="department IN ('sales', 'marketing', 'engineering') AND status != 'terminated'",
        sample_size=500,
        seed=42,  # For reproducibility
        output_branch_name="tech_departments_sample"
    )
    print(f"Job ID: {result['job_id']}")
    print()
    
    # Example 3: Pattern matching with LIKE
    print("Example 3: Email pattern matching")
    result = client.create_sampling_job(
        dataset_id=1,
        filter_expression="email LIKE '%@gmail.com' AND created_at > '2024-01-01'",
        sample_size=200,
        columns=["id", "email", "name", "created_at"],
        order_by="created_at",
        order_desc=True
    )
    print(f"Job ID: {result['job_id']}")
    print()
    
    # Example 4: Stratified sampling with filters
    print("Example 4: Stratified sampling by department with filters")
    result = client.create_stratified_sampling_job(
        dataset_id=1,
        filter_expression="status = 'active' AND salary > 50000",
        strata_columns=["department"],
        sample_size=1000,
        output_branch_name="stratified_high_earners"
    )
    print(f"Job ID: {result['job_id']}")
    print()
    
    # Example 5: Multi-round sampling with different filters
    print("Example 5: Multi-round sampling")
    rounds = [
        {
            "round_number": 1,
            "method": "random",
            "parameters": {
                "sample_size": 200,
                "filters": {
                    "expression": "department = 'engineering' AND role IN ('senior', 'lead', 'principal')"
                }
            },
            "output_name": "Senior Engineers"
        },
        {
            "round_number": 2,
            "method": "random",
            "parameters": {
                "sample_size": 300,
                "filters": {
                    "expression": "department = 'sales' AND quota_achievement > 100"
                }
            },
            "output_name": "Top Sales Performers"
        },
        {
            "round_number": 3,
            "method": "random",
            "parameters": {
                "sample_size": 100,
                "filters": {
                    "expression": "role = 'manager' AND direct_reports > 5"
                }
            },
            "output_name": "Large Team Managers"
        }
    ]
    
    result = client.create_multi_round_sampling(
        dataset_id=1,
        rounds=rounds,
        output_branch_name="multi_segment_analysis"
    )
    print(f"Job ID: {result['job_id']}")
    print()
    
    # Example 6: Complex nested conditions
    print("Example 6: Complex nested filter conditions")
    complex_filter = """
        ((age >= 25 AND age <= 45) AND department IN ('sales', 'marketing')) 
        OR (role = 'director' AND tenure_years > 5)
        OR (is_vip = 'true' AND total_purchases > 10000)
    """
    
    result = client.create_sampling_job(
        dataset_id=1,
        filter_expression=complex_filter.strip(),
        sample_size=1500,
        output_branch_name="complex_segment"
    )
    print(f"Job ID: {result['job_id']}")
    print()
    
    # Example 7: NULL checks and NOT operators
    print("Example 7: NULL checks and exclusions")
    result = client.create_sampling_job(
        dataset_id=1,
        filter_expression="manager_id IS NOT NULL AND department NOT IN ('temp', 'contractor') AND email NOT LIKE '%@test.com'",
        sample_size=800,
        output_branch_name="permanent_employees_with_managers"
    )
    print(f"Job ID: {result['job_id']}")
    
    print("\nAll sampling jobs created successfully!")