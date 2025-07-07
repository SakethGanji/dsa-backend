# DSA Platform Makefile

.PHONY: help test test-import test-import-bash test-import-python server worker db-migrate clean

# Default target
help:
	@echo "DSA Platform - Available commands:"
	@echo "  make server          - Start the FastAPI server"
	@echo "  make worker          - Start the job worker"
	@echo "  make test-import     - Run all import tests (Python version)"
	@echo "  make test-import-bash - Run import tests (Bash version)"
	@echo "  make test-import-python - Run import tests (Python version)"
	@echo "  make db-migrate      - Apply database migrations"
	@echo "  make clean           - Clean temporary files"

# Start the server
server:
	python -m uvicorn src.main:app --reload --host 0.0.0.0 --port 8000

# Start the worker (if running separately)
worker:
	python src/workers/run_worker.py

# Run import tests (default to Python version)
test-import: test-import-python

# Run import tests - Bash version
test-import-bash:
	@echo "Running import tests (Bash version)..."
	@bash tests/test_import_endpoints.sh

# Run import tests - Python version
test-import-python:
	@echo "Running import tests (Python version)..."
	@python3 tests/test_import_endpoints.py

# Run import tests with custom parameters
test-import-custom:
	@echo "Running import tests with custom parameters..."
	@echo "Usage: make test-import-custom ARGS='--dataset-id 2 --ref-name develop'"
	@python3 tests/test_import_endpoints.py $(ARGS)

# Apply database migrations
db-migrate:
	@echo "Applying import performance optimizations..."
	@python3 src/migrations/apply_import_optimizations.py

# Clean temporary files
clean:
	@echo "Cleaning temporary files..."
	@rm -f /tmp/test_import_*.csv /tmp/test_import_*.xlsx
	@rm -f /tmp/import_*.csv /tmp/import_*.xlsx
	@find . -type f -name "*.pyc" -delete
	@find . -type d -name "__pycache__" -delete

# Run all tests
test-all: test-import
	@echo "All tests completed"