# Import Endpoint Test Suite

This directory contains comprehensive test suites for the DSA platform's import functionality.

## Available Test Scripts

### 1. Python Test Suite (`test_import_endpoints.py`)
A comprehensive Python-based test suite with detailed verification and progress tracking.

**Features:**
- Colored output for easy reading
- Progress tracking during imports
- Database verification
- Edge case testing
- Detailed error reporting

**Usage:**
```bash
# Run with defaults
python3 tests/test_import_endpoints.py

# Run with custom parameters
python3 tests/test_import_endpoints.py \
  --api-url http://localhost:8000 \
  --username bg54677 \
  --password string \
  --dataset-id 1 \
  --ref-name main
```

### 2. Bash Test Suite (`test_import_endpoints.sh`)
A shell script version for quick testing and CI/CD integration.

**Features:**
- No Python dependencies (except for JSON parsing)
- Easy to integrate with CI/CD pipelines
- Colored output
- Database verification using psql

**Usage:**
```bash
# Run with defaults
./tests/test_import_endpoints.sh

# Run with environment variables
API_BASE_URL=http://localhost:8000 \
USERNAME=bg54677 \
PASSWORD=string \
DATASET_ID=1 \
REF_NAME=main \
./tests/test_import_endpoints.sh
```

## Test Coverage

Both test suites cover:

### CSV Import Tests
1. **Small CSV** - 10 rows
2. **Medium CSV** - 1,000 rows
3. **Large CSV** - 25,000 rows (tests batching)

### Excel Import Tests
1. **Small Multi-sheet Excel** - 2 sheets, 15 total rows
2. **Large Multi-sheet Excel** - Multiple sheets with 20,000+ rows (tests batching)
3. **Many Sheets Excel** - 10 sheets (Python suite only)

### Edge Cases (Python suite only)
1. **Empty CSV** - Headers only, no data
2. **Special Characters** - Quotes, newlines, Unicode

## Running Tests with Make

The easiest way to run tests is using the Makefile:

```bash
# Run Python test suite (default)
make test-import

# Run Bash test suite
make test-import-bash

# Run with custom parameters
make test-import-custom ARGS='--dataset-id 2 --ref-name develop'

# Clean up test files
make clean
```

## Prerequisites

### For Python Test Suite
```bash
pip install requests pandas numpy psycopg2-binary openpyxl
```

### For Bash Test Suite
- curl
- python3 (for JSON parsing)
- psql (PostgreSQL client)

### Database Requirements
- PostgreSQL running on localhost:5432
- Default credentials: postgres/postgres
- DSA schema installed

## Test Output

### Successful Test Run
```
[INFO] Authenticating as bg54677...
[PASS] Authentication successful (user_id: 87)
[INFO] Running test: Small CSV (10 rows)
[INFO] Job ID: 8efee669-2f2d-4e4a-8965-1052c5205461
[INFO] Import completed: 10 rows, commit 20250707005857_f53c71e5
[PASS] Small CSV (10 rows) - All verifications passed
...
========== Test Summary ==========
Tests Passed: 8
Tests Failed: 0
Total Tests: 8

All tests passed!
```

### Failed Test
```
[INFO] Running test: Large CSV with batching (25,000 rows)
[FAIL] Row count mismatch: expected 25000, got 24999
[FAIL] Large CSV with batching (25,000 rows) - Job failed: Import error
```

## Continuous Integration

Add to your CI/CD pipeline:

```yaml
# GitHub Actions example
- name: Run Import Tests
  run: |
    make test-import
  env:
    DATABASE_URL: postgresql://postgres:postgres@localhost:5432/postgres
```

## Troubleshooting

### Authentication Fails
- Verify the API server is running
- Check username/password
- Ensure the user exists in the database

### Import Job Timeout
- Check if the worker is running
- Look at worker logs for errors
- Increase timeout in test script

### Database Verification Fails
- Ensure PostgreSQL is accessible
- Check database credentials
- Verify the schema is up to date

## Performance Expectations

With optimizations applied:
- Small files (< 1000 rows): < 2 seconds
- Medium files (1000-10000 rows): < 5 seconds
- Large files (25000+ rows): < 10 seconds
- Memory usage: Constant regardless of file size