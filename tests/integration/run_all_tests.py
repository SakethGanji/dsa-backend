#!/usr/bin/env python3
"""
Run all integration tests using pytest.

This script runs both dataset and versioning endpoint tests.
It provides a summary of results at the end.
"""

import sys
import subprocess
from pathlib import Path

def run_tests():
    """Run all integration tests."""
    test_dir = Path(__file__).parent
    
    # Run pytest with appropriate options
    cmd = [
        sys.executable, "-m", "pytest",
        str(test_dir),
        "-v",  # Verbose output
        "--tb=short",  # Short traceback format
        "--strict-markers",  # Strict marker enforcement
        "-p", "no:warnings",  # Disable warnings
        "--color=yes",  # Color output
    ]
    
    # Run the tests
    result = subprocess.run(cmd, cwd=test_dir)
    
    return result.returncode

if __name__ == "__main__":
    exit_code = run_tests()
    sys.exit(exit_code)