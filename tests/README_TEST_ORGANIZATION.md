# Test Organization

This directory contains all tests for the DSA platform, organized by type and purpose.

## Directory Structure

```
tests/
├── unit/                    # Unit tests for individual components
│   ├── core/               # Core module tests
│   │   └── services/       # Service layer tests
│   │       ├── file_processing/
│   │       └── statistics/
│   └── features/           # Feature handler tests
├── integration/            # Integration tests
│   ├── test_table_api.py   # Table API integration tests
│   ├── test_versioning_api.py # Versioning API tests
│   └── test_workflow.py    # End-to-end workflow tests
├── api/                    # API-specific tests (legacy)
│   ├── test_datasets.py
│   └── test_users.py
├── manual/                 # Manual testing scripts (not for CI/CD)
│   ├── test_api_endpoints_manual.py  # Manual API testing
│   └── test_api_mock_manual.py       # Mock API testing
├── utilities/              # Test utilities and helpers
│   ├── run_tests.py        # Main test runner
│   ├── verify_parsers.py   # Quick parser verification
│   └── verify_api.py       # Quick API verification
├── conftest.py             # Pytest configuration and fixtures
└── utils.py                # Shared test utilities
```

## Running Tests

### Run All Tests
```bash
cd tests/utilities
python3 run_tests.py
```

### Run Specific Test Categories
```bash
# Unit tests only
pytest tests/unit/ -v

# Integration tests only
pytest tests/integration/ -v

# Specific module tests
pytest tests/unit/core/services/file_processing/ -v
```

### Quick Verification
```bash
# Verify parsers work
python3 tests/utilities/verify_parsers.py

# Verify API structure
python3 tests/utilities/verify_api.py
```

### Manual Testing
```bash
# Test all API endpoints manually (requires running server)
python3 tests/manual/test_api_endpoints_manual.py

# Test with mocks (no server required)
python3 tests/manual/test_api_mock_manual.py
```

## Test Categories

### Unit Tests (`unit/`)
- Test individual components in isolation
- Use mocks for all dependencies
- Fast execution, no external dependencies
- Should cover all edge cases

### Integration Tests (`integration/`)
- Test multiple components working together
- May use real database connections (with test data)
- Test API endpoints with mocked services
- Verify component interactions

### Manual Tests (`manual/`)
- Scripts for manual testing during development
- Not included in CI/CD pipeline
- Useful for debugging and development
- May require running services

### Utilities (`utilities/`)
- Test runners and helpers
- Quick verification scripts
- Not actual tests themselves

## Writing New Tests

1. **Unit Tests**: Place in appropriate subdirectory under `unit/`
2. **Integration Tests**: Add to `integration/` with clear naming
3. **Use Fixtures**: Leverage `conftest.py` for shared fixtures
4. **Mock External Dependencies**: Use `unittest.mock` for isolation
5. **Async Tests**: Use `pytest.mark.asyncio` for async functions

## CI/CD Integration

The test suite is designed for CI/CD:
- Run `pytest tests/unit/ tests/integration/` for CI
- Exclude `manual/` directory from automated testing
- Use `pytest --cov=src` for coverage reports