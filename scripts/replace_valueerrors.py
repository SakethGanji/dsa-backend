#!/usr/bin/env python3
"""Script to replace ValueError exceptions with domain exceptions."""

import os
import re
from pathlib import Path
from typing import List, Tuple

# Categorize ValueError patterns
REPLACEMENTS = [
    # Entity not found patterns - need special handling
    # Will be handled separately due to complexity
    
    # Validation patterns - specific cases
    (r'raise ValueError\("Potential SQL injection detected"\)', r'raise ValidationException("Potential SQL injection detected", field="sql")'),
    (r'raise ValueError\("Script tags not allowed"\)', r'raise ValidationException("Script tags not allowed", field="content")'),
    (r'raise ValueError\("Invalid filename: path traversal detected"\)', r'raise ValidationException("Invalid filename: path traversal detected", field="filename")'),
    (r'raise ValueError\(f?"Invalid file extension.*?"\)', r'raise ValidationException("Invalid file extension", field="file_extension")'),
    (r'raise ValueError\("Password must contain.*?"\)', r'raise ValidationException(\g<0>.replace("ValueError", "").strip("()"), field="password")'),
    (r'raise ValueError\(f?"Invalid (.*?)\..*?"\)', r'raise ValidationException(\g<0>.replace("ValueError", "").strip("()"))'),
    (r'raise ValueError\("At least one field must be provided.*?"\)', r'raise ValidationException("At least one field must be provided for update")'),
    
    # Conflict patterns
    (r'raise ValueError\(f?".*?already exists"?\)', lambda m: f'raise ConflictException({m.group(0).replace("ValueError", "").strip("()")})'),
    (r'raise ValueError\("Concurrent modification detected.*?"\)', r'raise ConflictException("Concurrent modification detected. Please retry.")'),
    
    # Business rule violations
    (r'raise ValueError\("Cannot delete.*?"\)', lambda m: f'raise BusinessRuleViolation({m.group(0).replace("ValueError", "").strip("()")})'),
    (r'raise ValueError\(f?"Cannot cancel job.*?"\)', lambda m: f'raise BusinessRuleViolation({m.group(0).replace("ValueError", "").strip("()")})'),
    
    # Generic validation for remaining
    (r'raise ValueError\((.*?)\)', r'raise ValidationException(\1)'),
]

# Files to update imports
IMPORT_UPDATES = {
    "EntityNotFoundException": "from src.core.domain_exceptions import EntityNotFoundException",
    "ValidationException": "from src.core.domain_exceptions import ValidationException",
    "ConflictException": "from src.core.domain_exceptions import ConflictException",
    "BusinessRuleViolation": "from src.core.domain_exceptions import BusinessRuleViolation",
    "ForbiddenException": "from src.core.domain_exceptions import ForbiddenException"
}

def update_file(filepath: Path, dry_run: bool = False) -> List[str]:
    """Update a single file."""
    with open(filepath, 'r') as f:
        content = f.read()
    
    original = content
    changes = []
    used_exceptions = set()
    
    # Apply replacements
    for pattern, replacement in REPLACEMENTS:
        if callable(replacement):
            # Lambda replacement
            new_content = re.sub(pattern, replacement, content)
        else:
            new_content = re.sub(pattern, replacement, content)
        
        if new_content != content:
            # Track which exceptions we're using
            for exc_name in IMPORT_UPDATES.keys():
                if exc_name in new_content and exc_name not in content:
                    used_exceptions.add(exc_name)
            
            # Find what changed
            matches = re.findall(pattern, content)
            for match in matches:
                changes.append(f"  - {pattern} -> {replacement if not callable(replacement) else 'custom replacement'}")
            
            content = new_content
    
    # Add necessary imports
    if used_exceptions and content != original:
        # Find the last import line
        lines = content.split('\n')
        last_import_idx = -1
        for i, line in enumerate(lines):
            if line.startswith('import ') or line.startswith('from '):
                last_import_idx = i
        
        # Check if imports already exist
        for exc_name in used_exceptions:
            if f"import {exc_name}" not in content and f"from src.core.domain_exceptions" not in content:
                if last_import_idx >= 0:
                    lines.insert(last_import_idx + 1, IMPORT_UPDATES[exc_name])
                    last_import_idx += 1
        
        content = '\n'.join(lines)
    
    # Write back if changed
    if content != original:
        if not dry_run:
            # Backup first
            backup_path = filepath.with_suffix(filepath.suffix + '.bak')
            with open(backup_path, 'w') as f:
                f.write(original)
            
            # Write updated content
            with open(filepath, 'w') as f:
                f.write(content)
        
        return changes
    
    return []

def main():
    """Main function."""
    print("Searching for ValueError exceptions to replace...\n")
    
    # Find all Python files with ValueError
    src_dir = Path("src")
    files_with_valueerror = []
    
    for filepath in src_dir.rglob("*.py"):
        if "__pycache__" in str(filepath) or str(filepath).endswith(".bak"):
            continue
        
        with open(filepath, 'r') as f:
            content = f.read()
        
        if "raise ValueError" in content:
            files_with_valueerror.append(filepath)
    
    print(f"Found {len(files_with_valueerror)} files with ValueError exceptions\n")
    
    # Process each file
    total_changes = 0
    for filepath in files_with_valueerror:
        changes = update_file(filepath, dry_run=False)
        if changes:
            print(f"Updated {filepath}:")
            for change in changes:
                print(change)
            print()
            total_changes += len(changes)
    
    print(f"\nTotal replacements: {total_changes}")
    print("\nBackup files created with .bak extension")
    print("Run 'find ./src -name '*.bak' -delete' to remove backups after verification")

if __name__ == "__main__":
    main()