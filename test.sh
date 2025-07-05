#!/bin/bash
# Main test runner for DSA platform

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🧪 DSA Platform Test Suite${NC}"
echo "============================"

# Default to running all tests
TEST_TYPE=${1:-all}

case $TEST_TYPE in
    unit)
        echo -e "${GREEN}Running unit tests...${NC}"
        python3 -m pytest tests/unit/ -v
        ;;
    integration)
        echo -e "${GREEN}Running integration tests...${NC}"
        python3 -m pytest tests/integration/ -v
        ;;
    coverage)
        echo -e "${GREEN}Running tests with coverage...${NC}"
        python3 -m pytest tests/unit/ tests/integration/ --cov=src --cov-report=term-missing
        ;;
    verify)
        echo -e "${GREEN}Running verification scripts...${NC}"
        python3 tests/utilities/verify_parsers.py
        echo ""
        python3 tests/utilities/verify_api.py
        ;;
    all)
        echo -e "${GREEN}Running all tests...${NC}"
        python3 tests/utilities/run_tests.py
        ;;
    *)
        echo "Usage: $0 [unit|integration|coverage|verify|all]"
        echo "  unit        - Run unit tests only"
        echo "  integration - Run integration tests only"
        echo "  coverage    - Run tests with coverage report"
        echo "  verify      - Run quick verification scripts"
        echo "  all         - Run all tests (default)"
        exit 1
        ;;
esac