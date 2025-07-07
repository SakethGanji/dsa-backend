#!/usr/bin/env python3
"""
Import Endpoints Test Suite

This script provides comprehensive testing for the DSA import functionality.
It tests various file types, sizes, and edge cases.
"""

import os
import sys
import time
import json
import csv
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
import tempfile
from pathlib import Path
import argparse
import psycopg2
from psycopg2.extras import RealDictCursor


class Colors:
    """ANSI color codes for terminal output"""
    GREEN = '\033[92m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    YELLOW = '\033[93m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


class ImportTestSuite:
    """Test suite for import endpoints"""
    
    def __init__(self, api_base_url: str, username: str, password: str, 
                 dataset_id: int = 1, ref_name: str = "main"):
        self.api_base_url = api_base_url.rstrip('/')
        self.username = username
        self.password = password
        self.dataset_id = dataset_id
        self.ref_name = ref_name
        self.token = None
        self.tests_passed = 0
        self.tests_failed = 0
        self.temp_files = []
        
        # Database connection
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'user': 'postgres',
            'password': 'postgres',
            'database': 'postgres'
        }
    
    def print_info(self, message: str):
        """Print info message"""
        print(f"{Colors.BLUE}[INFO]{Colors.ENDC} {message}")
    
    def print_success(self, message: str):
        """Print success message"""
        print(f"{Colors.GREEN}[PASS]{Colors.ENDC} {message}")
        self.tests_passed += 1
    
    def print_error(self, message: str):
        """Print error message"""
        print(f"{Colors.RED}[FAIL]{Colors.ENDC} {message}")
        self.tests_failed += 1
    
    def print_warning(self, message: str):
        """Print warning message"""
        print(f"{Colors.YELLOW}[WARN]{Colors.ENDC} {message}")
    
    def authenticate(self) -> bool:
        """Authenticate and get access token"""
        self.print_info(f"Authenticating as {self.username}...")
        
        try:
            response = requests.post(
                f"{self.api_base_url}/api/users/login",
                data={
                    'username': self.username,
                    'password': self.password
                }
            )
            response.raise_for_status()
            
            auth_data = response.json()
            self.token = auth_data['access_token']
            self.print_success(f"Authentication successful (user_id: {auth_data['user_id']})")
            return True
            
        except Exception as e:
            self.print_error(f"Authentication failed: {str(e)}")
            return False
    
    def create_test_csv(self, rows: int, filename: str) -> str:
        """Create a test CSV file"""
        filepath = os.path.join(tempfile.gettempdir(), filename)
        self.temp_files.append(filepath)
        
        departments = ['Engineering', 'Marketing', 'Sales', 'HR', 'Finance', 'IT', 'Legal', 'Operations']
        cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 
                  'San Antonio', 'San Diego', 'Dallas', 'San Jose']
        
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'name', 'age', 'city', 'department', 'salary', 'email', 'hire_date'])
            
            for i in range(1, rows + 1):
                writer.writerow([
                    i,
                    f'Employee_{i}',
                    np.random.randint(22, 65),
                    np.random.choice(cities),
                    np.random.choice(departments),
                    np.random.randint(40000, 150000),
                    f'employee{i}@company.com',
                    (datetime.now() - timedelta(days=np.random.randint(0, 3650))).strftime('%Y-%m-%d')
                ])
        
        return filepath
    
    def create_test_excel(self, sheets_config: Dict[str, int], filename: str) -> str:
        """Create a test Excel file with multiple sheets"""
        filepath = os.path.join(tempfile.gettempdir(), filename)
        self.temp_files.append(filepath)
        
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            for sheet_name, row_count in sheets_config.items():
                if sheet_name == 'Employees':
                    data = {
                        'employee_id': range(1, row_count + 1),
                        'name': [f'Employee {i}' for i in range(1, row_count + 1)],
                        'department': np.random.choice(['Engineering', 'Sales', 'Marketing', 'HR', 'Finance'], row_count),
                        'salary': np.random.randint(40000, 150000, row_count),
                        'performance_score': np.random.uniform(3.0, 5.0, row_count).round(2)
                    }
                elif sheet_name == 'Products':
                    data = {
                        'sku': [f'SKU{str(i).zfill(5)}' for i in range(row_count)],
                        'product_name': [f'Product {i}' for i in range(row_count)],
                        'category': np.random.choice(['Electronics', 'Clothing', 'Home', 'Sports'], row_count),
                        'price': np.random.uniform(10.0, 1000.0, row_count).round(2),
                        'stock': np.random.randint(0, 1000, row_count)
                    }
                elif sheet_name == 'Orders':
                    data = {
                        'order_id': range(100000, 100000 + row_count),
                        'customer_id': [f'CUST{str(i % 1000).zfill(4)}' for i in range(row_count)],
                        'order_date': [(datetime.now() - timedelta(days=i % 365)).strftime('%Y-%m-%d') for i in range(row_count)],
                        'total_amount': np.random.uniform(10.0, 5000.0, row_count).round(2),
                        'status': np.random.choice(['Pending', 'Shipped', 'Delivered', 'Cancelled'], row_count)
                    }
                else:
                    # Generic data for other sheets
                    data = {
                        'id': range(1, row_count + 1),
                        'value': np.random.randint(1, 1000, row_count),
                        'category': np.random.choice(['A', 'B', 'C', 'D'], row_count),
                        'timestamp': [(datetime.now() - timedelta(hours=i)).isoformat() for i in range(row_count)]
                    }
                
                pd.DataFrame(data).to_excel(writer, sheet_name=sheet_name, index=False)
        
        return filepath
    
    def import_file(self, filepath: str, commit_message: str) -> Optional[str]:
        """Import a file and return job ID"""
        try:
            with open(filepath, 'rb') as f:
                response = requests.post(
                    f"{self.api_base_url}/api/datasets/{self.dataset_id}/refs/{self.ref_name}/import",
                    headers={'Authorization': f'Bearer {self.token}'},
                    files={'file': (os.path.basename(filepath), f)},
                    data={'commit_message': commit_message}
                )
                response.raise_for_status()
                
                result = response.json()
                return result['job_id']
                
        except Exception as e:
            self.print_error(f"Failed to import file: {str(e)}")
            return None
    
    def wait_for_job(self, job_id: str, timeout: int = 60) -> Dict:
        """Wait for job completion and return final status"""
        start_time = time.time()
        last_progress = None
        
        while time.time() - start_time < timeout:
            try:
                response = requests.get(
                    f"{self.api_base_url}/api/jobs/{job_id}",
                    headers={'Authorization': f'Bearer {self.token}'}
                )
                response.raise_for_status()
                
                job_status = response.json()
                status = job_status['status']
                
                # Show progress if available
                progress = job_status.get('run_parameters', {}).get('progress', {})
                if progress and progress != last_progress:
                    self.print_info(f"Progress: {progress.get('status', 'Processing...')}")
                    last_progress = progress
                
                if status in ['completed', 'failed']:
                    return job_status
                
                time.sleep(2)
                
            except Exception as e:
                self.print_error(f"Error checking job status: {str(e)}")
                return {'status': 'error', 'error_message': str(e)}
        
        return {'status': 'timeout', 'error_message': f'Job timed out after {timeout} seconds'}
    
    def verify_import(self, commit_id: str, expected_rows: int, 
                     expected_sheets: Optional[Dict[str, int]] = None) -> bool:
        """Verify import results in database"""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Check total row count
                    cursor.execute("""
                        SELECT COUNT(*) as count 
                        FROM dsa_core.commit_rows 
                        WHERE commit_id = %s
                    """, (commit_id,))
                    
                    actual_rows = cursor.fetchone()['count']
                    
                    if actual_rows != expected_rows:
                        self.print_error(f"Row count mismatch: expected {expected_rows}, got {actual_rows}")
                        return False
                    
                    # Check sheet distribution if provided
                    if expected_sheets:
                        cursor.execute("""
                            SELECT 
                                SPLIT_PART(logical_row_id, ':', 1) as sheet_name,
                                COUNT(*) as row_count
                            FROM dsa_core.commit_rows 
                            WHERE commit_id = %s
                            GROUP BY SPLIT_PART(logical_row_id, ':', 1)
                            ORDER BY sheet_name
                        """, (commit_id,))
                        
                        sheet_counts = {row['sheet_name']: row['row_count'] 
                                      for row in cursor.fetchall()}
                        
                        for sheet, expected_count in expected_sheets.items():
                            actual_count = sheet_counts.get(sheet, 0)
                            if actual_count != expected_count:
                                self.print_error(f"Sheet '{sheet}' row count mismatch: expected {expected_count}, got {actual_count}")
                                return False
                    
                    return True
                    
        except Exception as e:
            self.print_error(f"Database verification failed: {str(e)}")
            return False
    
    def run_test(self, test_name: str, filepath: str, expected_rows: int,
                 expected_sheets: Optional[Dict[str, int]] = None) -> bool:
        """Run a single import test"""
        self.print_info(f"Running test: {test_name}")
        
        # Import file
        job_id = self.import_file(filepath, test_name)
        if not job_id:
            return False
        
        self.print_info(f"Job ID: {job_id}")
        
        # Wait for completion
        job_status = self.wait_for_job(job_id, timeout=120)
        
        if job_status['status'] == 'completed':
            rows_imported = job_status['output_summary']['rows_imported']
            commit_id = job_status['output_summary']['commit_id']
            
            self.print_info(f"Import completed: {rows_imported} rows, commit {commit_id}")
            
            # Verify in database
            if self.verify_import(commit_id, expected_rows, expected_sheets):
                self.print_success(f"{test_name} - All verifications passed")
                return True
            else:
                return False
        else:
            error_msg = job_status.get('error_message', 'Unknown error')
            self.print_error(f"{test_name} - Job failed: {error_msg}")
            return False
    
    def run_all_tests(self):
        """Run all import tests"""
        print(f"\n{Colors.BOLD}========== DSA Import Test Suite =========={Colors.ENDC}")
        print(f"API URL: {self.api_base_url}")
        print(f"Dataset ID: {self.dataset_id}")
        print(f"Target Ref: {self.ref_name}")
        print("=" * 42)
        
        if not self.authenticate():
            return
        
        print(f"\n{Colors.BOLD}CSV Import Tests{Colors.ENDC}")
        print("-" * 20)
        
        # Test 1: Small CSV
        csv_small = self.create_test_csv(10, 'test_small.csv')
        self.run_test("Small CSV (10 rows)", csv_small, 10)
        
        # Test 2: Medium CSV
        csv_medium = self.create_test_csv(1000, 'test_medium.csv')
        self.run_test("Medium CSV (1,000 rows)", csv_medium, 1000)
        
        # Test 3: Large CSV (tests batching)
        csv_large = self.create_test_csv(25000, 'test_large.csv')
        self.run_test("Large CSV with batching (25,000 rows)", csv_large, 25000)
        
        print(f"\n{Colors.BOLD}Excel Import Tests{Colors.ENDC}")
        print("-" * 20)
        
        # Test 4: Small multi-sheet Excel
        excel_small = self.create_test_excel(
            {'Employees': 10, 'Products': 5}, 
            'test_small.xlsx'
        )
        self.run_test("Small Excel (2 sheets, 15 rows)", excel_small, 15,
                     {'Employees': 10, 'Products': 5})
        
        # Test 5: Large multi-sheet Excel (tests batching)
        excel_large = self.create_test_excel(
            {'Orders': 15000, 'Products': 5000, 'Employees': 3000}, 
            'test_large.xlsx'
        )
        self.run_test("Large Excel with batching (3 sheets, 23,000 rows)", excel_large, 23000,
                     {'Orders': 15000, 'Products': 5000, 'Employees': 3000})
        
        # Test 6: Excel with many sheets
        excel_many = self.create_test_excel(
            {f'Sheet{i}': 100 for i in range(1, 11)}, 
            'test_many_sheets.xlsx'
        )
        self.run_test("Excel with many sheets (10 sheets, 1,000 rows)", excel_many, 1000)
        
        print(f"\n{Colors.BOLD}Edge Case Tests{Colors.ENDC}")
        print("-" * 20)
        
        # Test 7: Empty CSV
        csv_empty = os.path.join(tempfile.gettempdir(), 'test_empty.csv')
        self.temp_files.append(csv_empty)
        with open(csv_empty, 'w') as f:
            f.write('id,name,value\n')  # Headers only
        self.run_test("Empty CSV (headers only)", csv_empty, 0)
        
        # Test 8: CSV with special characters
        csv_special = os.path.join(tempfile.gettempdir(), 'test_special.csv')
        self.temp_files.append(csv_special)
        with open(csv_special, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'name', 'description'])
            writer.writerow([1, 'Test "quoted"', 'Line 1\nLine 2'])
            writer.writerow([2, "O'Brien", 'Special chars: é, ñ, ü'])
            writer.writerow([3, 'Comma, test', 'Tab\there'])
        self.run_test("CSV with special characters", csv_special, 3)
        
        # Summary
        self.print_summary()
        self.cleanup()
    
    def print_summary(self):
        """Print test summary"""
        print(f"\n{Colors.BOLD}========== Test Summary =========={Colors.ENDC}")
        print(f"Tests Passed: {Colors.GREEN}{self.tests_passed}{Colors.ENDC}")
        print(f"Tests Failed: {Colors.RED}{self.tests_failed}{Colors.ENDC}")
        print(f"Total Tests: {self.tests_passed + self.tests_failed}")
        print("=" * 34)
        
        if self.tests_failed == 0:
            print(f"\n{Colors.GREEN}{Colors.BOLD}All tests passed!{Colors.ENDC}")
        else:
            print(f"\n{Colors.RED}{Colors.BOLD}Some tests failed!{Colors.ENDC}")
    
    def cleanup(self):
        """Clean up temporary files"""
        self.print_info("Cleaning up temporary files...")
        for filepath in self.temp_files:
            try:
                if os.path.exists(filepath):
                    os.remove(filepath)
            except Exception as e:
                self.print_warning(f"Failed to remove {filepath}: {str(e)}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Test DSA Import Endpoints')
    parser.add_argument('--api-url', default='http://localhost:8000',
                        help='API base URL (default: http://localhost:8000)')
    parser.add_argument('--username', default='bg54677',
                        help='Username for authentication (default: bg54677)')
    parser.add_argument('--password', default='string',
                        help='Password for authentication (default: string)')
    parser.add_argument('--dataset-id', type=int, default=1,
                        help='Dataset ID to import into (default: 1)')
    parser.add_argument('--ref-name', default='main',
                        help='Reference name to update (default: main)')
    
    args = parser.parse_args()
    
    # Run test suite
    test_suite = ImportTestSuite(
        api_base_url=args.api_url,
        username=args.username,
        password=args.password,
        dataset_id=args.dataset_id,
        ref_name=args.ref_name
    )
    
    test_suite.run_all_tests()
    
    # Exit with appropriate code
    sys.exit(0 if test_suite.tests_failed == 0 else 1)


if __name__ == '__main__':
    main()