# DSA Platform Tests

This directory contains the test suite for the DSA Platform API.

## Test Structure

```
tests/
├── api/              # API endpoint tests
│   ├── test_users.py    # User authentication and registration tests
│   └── test_datasets.py # Dataset and permission management tests
├── integration/      # Integration tests
│   └── test_workflow.py # Complete workflow tests
├── unit/            # Unit tests (to be added)
├── conftest.py      # Pytest configuration and fixtures
├── utils.py         # Test utilities and helpers
└── README.md        # This file
```

## Running Tests

### Prerequisites

1. PostgreSQL database running with the DSA schema
2. Environment variables set:
   ```bash
   export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
   export SECRET_KEY="test-secret-key-for-testing-only"
   ```

### Run All Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/api/test_users.py

# Run specific test class
pytest tests/api/test_users.py::TestUserAuthentication

# Run specific test
pytest tests/api/test_users.py::TestUserAuthentication::test_login_success
```

### Test Categories

Use markers to run specific categories of tests:

```bash
# Run only API tests
pytest -m api

# Run only integration tests
pytest -m integration

# Skip slow tests
pytest -m "not slow"

# Run only unit tests
pytest -m unit
```

## Test Fixtures

Key fixtures available in `conftest.py`:

- `client`: Async HTTP client for making API requests
- `db_session`: Database session with automatic rollback
- `test_user`: Creates a test user with admin role
- `auth_headers`: Provides authorization headers with valid JWT token
- `test_dataset`: Creates a test dataset with permissions

## Writing Tests

### API Test Example

```python
async def test_create_dataset(client: AsyncClient, auth_headers: dict):
    response = await client.post(
        "/api/datasets/",
        json={"name": "test", "description": "Test dataset"},
        headers=auth_headers
    )
    assert response.status_code == 200
    assert response.json()["name"] == "test"
```

### Database Test Example

```python
async def test_user_creation(db_session):
    user_id = await db_session.fetchval("""
        INSERT INTO dsa_auth.users (soeid, password_hash, role_id)
        VALUES ('TEST123', 'hash', 1)
        RETURNING id
    """)
    assert user_id is not None
```

## Test Database

Tests use the same database as development but with automatic transaction rollback:
- Each test runs in its own transaction
- All changes are rolled back after the test
- No cleanup needed between tests

## Best Practices

1. **Use fixtures**: Leverage the provided fixtures for common setup
2. **Test isolation**: Each test should be independent
3. **Clear assertions**: Use descriptive assertion messages
4. **Async/await**: Remember to use `async def` for test functions
5. **Meaningful names**: Use descriptive test function names

## Coverage

Generate coverage reports:

```bash
# Terminal report
pytest --cov=src --cov-report=term-missing

# HTML report
pytest --cov=src --cov-report=html
# Open htmlcov/index.html in browser

# XML report (for CI)
pytest --cov=src --cov-report=xml
```

## Debugging

```bash
# Run with verbose output
pytest -v

# Show print statements
pytest -s

# Drop into debugger on failure
pytest --pdb

# Run specific test with full traceback
pytest -vvv tests/api/test_users.py::test_login_success
```

## CI/CD Integration

The test suite is designed to run in CI/CD pipelines:

```yaml
# Example GitHub Actions
- name: Run tests
  run: |
    pytest --cov=src --cov-report=xml
    
- name: Upload coverage
  uses: codecov/codecov-action@v3
```