#!/usr/bin/env python3
"""Run tests with proper async configuration."""

import subprocess
import sys

# Run pytest with asyncio mode
cmd = [
    sys.executable, "-m", "pytest",
    "--asyncio-mode=auto",
    "-v",
    "--tb=short",
    "--no-cov"  # Disable coverage for now
] + sys.argv[1:]

sys.exit(subprocess.call(cmd))