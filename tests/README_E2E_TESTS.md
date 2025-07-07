# E2E API Tests for DSA Platform

This directory contains comprehensive end-to-end tests for all API endpoints, including the table analysis endpoint and multi-sheet Excel support.

## Test Coverage

The tests cover all phases of the API workflow:

1. **Phase 0**: Health checks
2. **Phase 1**: User authentication (registration, login, token validation)
3. **Phase 2**: Dataset CRUD operations
4. **Phase 3**: Multi-sheet Excel file creation
5. **Phase 4**: Data versioning, table analysis, and multi-sheet validation
6. **Phase 5**: Search and discovery
7. **Phase 6**: Asynchronous operations (sampling jobs)
8. **Phase 7**: Cleanup

## Prerequisites

- DSA platform running on `http://localhost:8000`
- PostgreSQL database configured (default: `postgresql://postgres:postgres@localhost:5432/postgres`)
- Python 3.8+ with pandas and openpyxl installed

## Running the Tests

### Option 1: Bash Script (using curl)
```bash
cd tests
./e2e_api_test.sh
```

### Option 2: Python Script (more detailed output)
```bash
cd tests
python e2e_api_test.py
```

## Test User Credentials

- Email: `bg54677@test.com`
- Password: `string`

## Key Features Tested

### Table Analysis Endpoint
- Tests the `/api/datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/analysis` endpoint
- Validates response structure including:
  - Column types
  - Null counts
  - Sample values
  - Total rows
  - Statistics

### Multi-Sheet Excel Support
- Creates a test Excel file with 3 sheets:
  - Sales data (with dates, products, regions)
  - Customer data (with IDs, emails, categories)
  - Product inventory (with stock levels, prices)
- Validates that all sheets are imported as separate tables
- Tests analysis endpoint for each imported table

## Expected Output

Successful test run should show:
```
✓ All tests passed!
Total Tests: 38
Passed: 38
Failed: 0
```

## Troubleshooting

1. **Connection refused**: Ensure the API server is running on port 8000
2. **401 Unauthorized**: Check that the authentication endpoints are working
3. **Import failures**: Verify that pandas and openpyxl are installed for Excel file creation
4. **Database errors**: Check PostgreSQL connection settings in `src/core/config.py`

## Test Data

The tests create temporary data including:
- A test user account
- A test dataset
- A multi-sheet Excel file with sample data
- Sampling jobs

All test data is cleaned up automatically at the end of the test run.