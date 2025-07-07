#!/bin/bash

# Import Endpoints Test Suite
# This script tests all import functionality with various file types and sizes

set -e  # Exit on error

# Configuration
API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"
USERNAME="${USERNAME:-bg54677}"
PASSWORD="${PASSWORD:-string}"
DATASET_ID="${DATASET_ID:-1}"
REF_NAME="${REF_NAME:-main}"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test results tracking
TESTS_PASSED=0
TESTS_FAILED=0

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[PASS]${NC} $1"
    ((TESTS_PASSED++))
}

print_error() {
    echo -e "${RED}[FAIL]${NC} $1"
    ((TESTS_FAILED++))
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Function to authenticate and get token
authenticate() {
    print_info "Authenticating as $USERNAME..."
    
    AUTH_RESPONSE=$(curl -s -X POST "$API_BASE_URL/api/users/login" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "username=$USERNAME&password=$PASSWORD")
    
    TOKEN=$(echo "$AUTH_RESPONSE" | python3 -c "import sys, json; print(json.load(sys.stdin)['access_token'])" 2>/dev/null)
    
    if [ -z "$TOKEN" ]; then
        print_error "Authentication failed"
        echo "$AUTH_RESPONSE"
        exit 1
    fi
    
    print_success "Authentication successful"
    export TOKEN
}

# Function to create test CSV files
create_test_csv_files() {
    print_info "Creating test CSV files..."
    
    # Small CSV (10 rows)
    cat > /tmp/test_import_small.csv << 'EOF'
id,name,age,city,department
1,John Doe,28,New York,Engineering
2,Jane Smith,32,Los Angeles,Marketing
3,Bob Johnson,45,Chicago,Sales
4,Alice Williams,29,Houston,Engineering
5,Charlie Brown,38,Phoenix,HR
6,Diana Davis,41,Philadelphia,Finance
7,Edward Miller,33,San Antonio,Engineering
8,Fiona Wilson,27,San Diego,Marketing
9,George Moore,52,Dallas,Sales
10,Helen Taylor,36,San Jose,HR
EOF
    
    # Medium CSV (1000 rows)
    python3 << 'EOF'
import csv
with open('/tmp/test_import_medium.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['id', 'name', 'age', 'city', 'department'])
    for i in range(1, 1001):
        writer.writerow([i, f'Person_{i}', 20 + (i % 45), f'City_{i % 20}', f'Dept_{i % 5}'])
EOF
    
    # Large CSV (25000 rows - tests batching)
    python3 << 'EOF'
import csv
import random
departments = ['Engineering', 'Marketing', 'Sales', 'HR', 'Finance', 'IT', 'Legal', 'Operations']
cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 
          'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville', 'San Francisco', 'Columbus']

with open('/tmp/test_import_large.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['id', 'name', 'age', 'city', 'department', 'salary', 'email'])
    for i in range(1, 25001):
        writer.writerow([
            i, 
            f'Employee_{i}', 
            random.randint(22, 65),
            random.choice(cities),
            random.choice(departments),
            random.randint(40000, 150000),
            f'employee{i}@company.com'
        ])
EOF
    
    print_success "Created test CSV files"
}

# Function to create test Excel files
create_test_excel_files() {
    print_info "Creating test Excel files..."
    
    # Small multi-sheet Excel
    python3 << 'EOF'
import pandas as pd
from datetime import datetime, timedelta

# Create Excel with 2 sheets
employees_data = {
    'employee_id': range(1, 11),
    'name': [f'Employee {i}' for i in range(1, 11)],
    'department': ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance'] * 2,
    'salary': [50000 + i * 5000 for i in range(10)],
    'hire_date': [(datetime.now() - timedelta(days=i*100)).strftime('%Y-%m-%d') for i in range(10)]
}

projects_data = {
    'project_id': range(101, 106),
    'project_name': ['Project Alpha', 'Project Beta', 'Project Gamma', 'Project Delta', 'Project Epsilon'],
    'budget': [100000, 150000, 200000, 175000, 125000],
    'status': ['Active', 'Completed', 'Active', 'Planning', 'Active']
}

with pd.ExcelWriter('/tmp/test_import_small.xlsx', engine='openpyxl') as writer:
    pd.DataFrame(employees_data).to_excel(writer, sheet_name='Employees', index=False)
    pd.DataFrame(projects_data).to_excel(writer, sheet_name='Projects', index=False)
EOF
    
    # Large multi-sheet Excel (tests batching)
    python3 << 'EOF'
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Sheet 1: Large Orders Dataset (15,000 rows)
orders_data = {
    'order_id': range(100000, 115000),
    'customer_id': [f'CUST{str(i % 1000).zfill(4)}' for i in range(15000)],
    'order_date': [(datetime.now() - timedelta(days=i % 365)).strftime('%Y-%m-%d') for i in range(15000)],
    'total_amount': np.random.uniform(10.0, 5000.0, 15000).round(2),
    'status': np.random.choice(['Pending', 'Shipped', 'Delivered', 'Cancelled'], 15000)
}

# Sheet 2: Products Catalog (5,000 rows)
products_data = {
    'sku': [f'SKU{str(i).zfill(5)}' for i in range(5000)],
    'product_name': [f'Product {i}' for i in range(5000)],
    'category': np.random.choice(['Electronics', 'Clothing', 'Home', 'Sports', 'Books'], 5000),
    'price': np.random.uniform(5.0, 1000.0, 5000).round(2)
}

with pd.ExcelWriter('/tmp/test_import_large.xlsx', engine='openpyxl') as writer:
    pd.DataFrame(orders_data).to_excel(writer, sheet_name='Orders', index=False)
    pd.DataFrame(products_data).to_excel(writer, sheet_name='Products', index=False)
EOF
    
    print_success "Created test Excel files"
}

# Function to test file import
test_import() {
    local file_path=$1
    local test_name=$2
    local expected_rows=$3
    
    print_info "Testing: $test_name"
    
    # Import file
    RESPONSE=$(curl -s -X POST "$API_BASE_URL/api/datasets/$DATASET_ID/refs/$REF_NAME/import" \
        -H "Authorization: Bearer $TOKEN" \
        -F "file=@$file_path" \
        -F "commit_message=$test_name")
    
    JOB_ID=$(echo "$RESPONSE" | python3 -c "import sys, json; print(json.load(sys.stdin)['job_id'])" 2>/dev/null)
    
    if [ -z "$JOB_ID" ]; then
        print_error "$test_name - Failed to queue import job"
        echo "$RESPONSE"
        return 1
    fi
    
    print_info "Job queued with ID: $JOB_ID"
    
    # Wait for job completion
    local max_attempts=30
    local attempt=0
    local status="pending"
    
    while [ "$status" = "pending" ] || [ "$status" = "running" ]; do
        sleep 2
        ((attempt++))
        
        if [ $attempt -gt $max_attempts ]; then
            print_error "$test_name - Job timeout after $((max_attempts * 2)) seconds"
            return 1
        fi
        
        JOB_STATUS=$(curl -s -X GET "$API_BASE_URL/api/jobs/$JOB_ID" \
            -H "Authorization: Bearer $TOKEN")
        
        status=$(echo "$JOB_STATUS" | python3 -c "import sys, json; print(json.load(sys.stdin)['status'])" 2>/dev/null)
        
        if [ "$status" = "running" ]; then
            progress=$(echo "$JOB_STATUS" | python3 -c "
import sys, json
data = json.load(sys.stdin)
progress = data.get('run_parameters', {}).get('progress', {})
if progress:
    print(f\"Progress: {progress.get('status', 'Processing...')}\")
" 2>/dev/null)
            [ -n "$progress" ] && print_info "$progress"
        fi
    done
    
    if [ "$status" = "completed" ]; then
        rows_imported=$(echo "$JOB_STATUS" | python3 -c "import sys, json; print(json.load(sys.stdin)['output_summary']['rows_imported'])" 2>/dev/null)
        commit_id=$(echo "$JOB_STATUS" | python3 -c "import sys, json; print(json.load(sys.stdin)['output_summary']['commit_id'])" 2>/dev/null)
        
        if [ "$rows_imported" = "$expected_rows" ]; then
            print_success "$test_name - Imported $rows_imported rows (commit: $commit_id)"
        else
            print_error "$test_name - Expected $expected_rows rows, got $rows_imported"
        fi
    else
        error_msg=$(echo "$JOB_STATUS" | python3 -c "import sys, json; print(json.load(sys.stdin).get('error_message', 'Unknown error'))" 2>/dev/null)
        print_error "$test_name - Job failed: $error_msg"
    fi
}

# Function to verify database state
verify_database() {
    print_info "Verifying database state..."
    
    # Check latest commit
    LATEST_COMMIT=$(PGPASSWORD=postgres psql -U postgres -h localhost -d postgres -t -c "
        SELECT c.commit_id
        FROM dsa_core.commits c
        JOIN dsa_core.refs r ON c.commit_id = r.commit_id
        WHERE r.dataset_id = $DATASET_ID AND r.name = '$REF_NAME'
        LIMIT 1;
    " 2>/dev/null | xargs)
    
    if [ -n "$LATEST_COMMIT" ]; then
        # Count rows in latest commit
        ROW_COUNT=$(PGPASSWORD=postgres psql -U postgres -h localhost -d postgres -t -c "
            SELECT COUNT(*)
            FROM dsa_core.commit_rows
            WHERE commit_id = '$LATEST_COMMIT';
        " 2>/dev/null | xargs)
        
        print_info "Latest commit: $LATEST_COMMIT has $ROW_COUNT rows"
        
        # Check sheet distribution for multi-sheet files
        PGPASSWORD=postgres psql -U postgres -h localhost -d postgres -c "
            SELECT 
                SPLIT_PART(logical_row_id, ':', 1) as sheet_name,
                COUNT(*) as row_count
            FROM dsa_core.commit_rows 
            WHERE commit_id = '$LATEST_COMMIT'
            GROUP BY SPLIT_PART(logical_row_id, ':', 1)
            ORDER BY sheet_name;
        " 2>/dev/null
    fi
}

# Main test execution
main() {
    echo "========================================="
    echo "DSA Import Endpoints Test Suite"
    echo "========================================="
    echo "API URL: $API_BASE_URL"
    echo "Dataset ID: $DATASET_ID"
    echo "Target Ref: $REF_NAME"
    echo "========================================="
    echo
    
    # Authenticate
    authenticate
    echo
    
    # Create test files
    create_test_csv_files
    create_test_excel_files
    echo
    
    # Run tests
    print_info "Starting import tests..."
    echo
    
    # CSV Tests
    test_import "/tmp/test_import_small.csv" "Small CSV Import (10 rows)" 10
    echo
    
    test_import "/tmp/test_import_medium.csv" "Medium CSV Import (1,000 rows)" 1000
    echo
    
    test_import "/tmp/test_import_large.csv" "Large CSV Import with Batching (25,000 rows)" 25000
    echo
    
    # Excel Tests
    test_import "/tmp/test_import_small.xlsx" "Multi-sheet Excel Import (15 rows, 2 sheets)" 15
    echo
    
    test_import "/tmp/test_import_large.xlsx" "Large Multi-sheet Excel with Batching (20,000 rows, 2 sheets)" 20000
    echo
    
    # Verify final database state
    verify_database
    echo
    
    # Summary
    echo "========================================="
    echo "Test Summary"
    echo "========================================="
    echo -e "Tests Passed: ${GREEN}$TESTS_PASSED${NC}"
    echo -e "Tests Failed: ${RED}$TESTS_FAILED${NC}"
    echo "========================================="
    
    # Cleanup
    print_info "Cleaning up test files..."
    rm -f /tmp/test_import_*.csv /tmp/test_import_*.xlsx
    
    # Exit with appropriate code
    if [ $TESTS_FAILED -gt 0 ]; then
        exit 1
    else
        exit 0
    fi
}

# Run main function
main