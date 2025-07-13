#!/usr/bin/env python3
"""Script to replace ValueError exceptions with domain exceptions - simple approach."""

import os
import re
from pathlib import Path

def categorize_valueerror(error_msg: str) -> tuple[str, str]:
    """Categorize ValueError and return (exception_type, formatted_message)."""
    
    # Entity not found patterns
    if "not found" in error_msg.lower():
        if "dataset" in error_msg.lower():
            return "EntityNotFoundException", f'EntityNotFoundException("Dataset", dataset_id)'
        elif "user" in error_msg.lower():
            return "EntityNotFoundException", f'EntityNotFoundException("User", user_id)'
        elif "job" in error_msg.lower():
            return "EntityNotFoundException", f'EntityNotFoundException("Job", job_id)'
        elif "commit" in error_msg.lower():
            return "EntityNotFoundException", f'EntityNotFoundException("Commit", commit_id)'
        elif "branch" in error_msg.lower() or "ref" in error_msg.lower():
            return "EntityNotFoundException", f'EntityNotFoundException("Branch", ref_name)'
        elif "schema" in error_msg.lower():
            return "EntityNotFoundException", f'EntityNotFoundException("Schema", schema_id)'
        else:
            return "EntityNotFoundException", f'EntityNotFoundException("Entity", entity_id)'
    
    # Conflict patterns
    elif "already exists" in error_msg.lower():
        return "ConflictException", f'ConflictException({error_msg})'
    elif "concurrent modification" in error_msg.lower():
        return "ConflictException", f'ConflictException({error_msg})'
    
    # Business rule violations
    elif "cannot delete" in error_msg.lower():
        return "BusinessRuleViolation", f'BusinessRuleViolation({error_msg})'
    elif "cannot cancel" in error_msg.lower():
        return "BusinessRuleViolation({error_msg})'
    
    # Validation errors (default)
    else:
        # Check for specific field validation
        if "password must" in error_msg.lower():
            return "ValidationException", f'ValidationException({error_msg}, field="password")'
        elif "invalid filename" in error_msg.lower():
            return "ValidationException", f'ValidationException({error_msg}, field="filename")'
        elif "sql injection" in error_msg.lower():
            return "ValidationException", f'ValidationException({error_msg}, field="sql")'
        else:
            return "ValidationException", f'ValidationException({error_msg})'

def update_file(filepath: Path) -> int:
    """Update a single file and return number of changes."""
    with open(filepath, 'r') as f:
        lines = f.readlines()
    
    original_lines = lines.copy()
    changes = 0
    used_exceptions = set()
    
    for i, line in enumerate(lines):
        if "raise ValueError(" in line:
            # Extract the ValueError content
            match = re.search(r'raise ValueError\((.*?)\)$', line.strip())
            if match:
                error_content = match.group(1)
                exc_type, new_exception = categorize_valueerror(error_content)
                
                # Replace the line
                indent = len(line) - len(line.lstrip())
                lines[i] = ' ' * indent + f'raise {new_exception}\n'
                
                used_exceptions.add(exc_type)
                changes += 1
                print(f"  Line {i+1}: ValueError({error_content}) -> {new_exception}")
    
    if changes > 0:
        # Add imports if needed
        import_added = False
        for i, line in enumerate(lines):
            if "from src.core.domain_exceptions import" in line:
                import_added = True
                break
        
        if not import_added and used_exceptions:
            # Find last import
            last_import = 0
            for i, line in enumerate(lines):
                if line.strip().startswith('import ') or line.strip().startswith('from '):
                    last_import = i
            
            # Add our import
            exc_imports = ", ".join(sorted(used_exceptions))
            lines.insert(last_import + 1, f"from src.core.domain_exceptions import {exc_imports}\n")
        
        # Backup and write
        backup_path = filepath.with_suffix(filepath.suffix + '.bak')
        with open(backup_path, 'w') as f:
            f.writelines(original_lines)
        
        with open(filepath, 'w') as f:
            f.writelines(lines)
    
    return changes

def main():
    """Main function."""
    print("Replacing ValueError exceptions with domain exceptions...\n")
    
    # Find files with ValueError
    files = []
    for filepath in Path("src").rglob("*.py"):
        if "__pycache__" in str(filepath) or str(filepath).endswith(".bak"):
            continue
        
        with open(filepath, 'r') as f:
            if "raise ValueError" in f.read():
                files.append(filepath)
    
    print(f"Found {len(files)} files with ValueError exceptions\n")
    
    # Process each file
    total_changes = 0
    for filepath in files:
        print(f"\nProcessing {filepath}:")
        changes = update_file(filepath)
        if changes:
            total_changes += changes
            print(f"  Made {changes} replacements")
        else:
            print("  No simple replacements found")
    
    print(f"\n\nTotal replacements: {total_changes}")
    print("\nBackup files created with .bak extension")
    print("Manual review recommended for complex patterns")

if __name__ == "__main__":
    main()