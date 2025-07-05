#!/bin/bash
# Run unit tests with proper configuration

echo "🧪 Running Unit Tests..."
echo "========================"

# Run from project root
cd "$(dirname "$0")/.."

# Run unit tests with coverage
python3 -m pytest tests/unit/ -v --cov=src --cov-report=term-missing

echo ""
echo "✅ Unit tests complete!"