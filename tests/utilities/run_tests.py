#!/usr/bin/env python
"""Test runner script for DSA platform."""

import sys
import subprocess
import os

def run_tests():
    """Run all tests and display results."""
    print("🧪 Running DSA Platform Tests...")
    print("=" * 60)
    
    # Set PYTHONPATH to include src directory
    env = os.environ.copy()
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    env['PYTHONPATH'] = os.path.join(project_root, 'src')
    
    # Test categories
    test_suites = [
        {
            'name': 'Unit Tests - File Processing',
            'path': 'tests/unit/core/services/file_processing/',
            'markers': None
        },
        {
            'name': 'Unit Tests - Statistics',
            'path': 'tests/unit/core/services/statistics/',
            'markers': None
        },
        {
            'name': 'Unit Tests - Features',
            'path': 'tests/unit/features/',
            'markers': None
        },
        {
            'name': 'Integration Tests - API',
            'path': 'tests/integration/',
            'markers': None
        }
    ]
    
    total_passed = 0
    total_failed = 0
    failed_suites = []
    
    for suite in test_suites:
        print(f"\n📁 Running {suite['name']}...")
        print("-" * 40)
        
        cmd = [
            sys.executable, '-m', 'pytest',
            suite['path'],
            '-v',
            '--tb=short',
            '--no-header'
        ]
        
        if suite['markers']:
            cmd.extend(['-m', suite['markers']])
        
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        
        # Parse output for test counts
        output_lines = result.stdout.split('\n')
        for line in output_lines:
            if 'passed' in line or 'failed' in line:
                print(line)
                
                # Extract counts
                if 'passed' in line:
                    try:
                        count = int(line.split()[0])
                        total_passed += count
                    except:
                        pass
                if 'failed' in line and 'passed' not in line:
                    try:
                        count = int(line.split()[0])
                        total_failed += count
                        failed_suites.append(suite['name'])
                    except:
                        pass
        
        # Show errors if any
        if result.returncode != 0:
            print("\n❌ Errors:")
            print(result.stderr)
            if 'FAILED' in result.stdout:
                # Extract failed test details
                in_failed_section = False
                for line in output_lines:
                    if 'FAILED' in line and '::' in line:
                        in_failed_section = True
                    if in_failed_section and line.strip():
                        print(f"  {line}")
                    if line.startswith('='):
                        in_failed_section = False
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 Test Summary")
    print("=" * 60)
    print(f"✅ Passed: {total_passed}")
    print(f"❌ Failed: {total_failed}")
    
    if failed_suites:
        print(f"\n⚠️  Failed suites: {', '.join(failed_suites)}")
    
    if total_failed == 0:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print(f"\n😞 {total_failed} tests failed.")
        return 1


if __name__ == "__main__":
    sys.exit(run_tests())