#!/bin/bash
# Run all API tests and generate a summary

echo "========================================="
echo "Running DSA API Tests"
echo "========================================="
echo ""

# Ensure database is set up
echo "Setting up test database..."
./scripts/setup_test_db.sh > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "Failed to set up test database"
    exit 1
fi

echo "Running tests..."
echo ""

# Run tests with summary
python3 scripts/run_tests.py tests/api/ -v --tb=short | tee test_results.log

# Extract summary
echo ""
echo "========================================="
echo "Test Summary"
echo "========================================="
grep -E "(PASSED|FAILED|ERROR)" test_results.log | sort | uniq -c | sort -nr

# Check if any tests failed
if grep -q "FAILED\|ERROR" test_results.log; then
    echo ""
    echo "Some tests failed. Check test_results.log for details."
    exit 1
else
    echo ""
    echo "All tests passed!"
    exit 0
fi