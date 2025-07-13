#!/usr/bin/env python3
"""Script to update model imports after reorganization."""

import os
import re
from pathlib import Path


# Define import mappings
IMPORT_MAPPINGS = {
    # Base models and constants
    r'from \.\.models\.base_models import': 'from ..core.abstractions.models import',
    r'from src\.models\.base_models import': 'from src.core.abstractions.models import',
    r'from models\.base_models import': 'from src.core.abstractions.models import',
    
    # API models
    r'from \.\.models\.pydantic_models import': 'from ..api.models import',
    r'from src\.models\.pydantic_models import': 'from src.api.models import',
    r'from models\.pydantic_models import': 'from src.api.models import',
    
    # Validation models
    r'from \.\.models\.validation_models import': 'from ..api.validation import',
    r'from src\.models\.validation_models import': 'from src.api.validation import',
    r'from models\.validation_models import': 'from src.api.validation import',
    
    # Response factories
    r'from \.\.models\.response_factories import': 'from ..api.factories import',
    r'from src\.models\.response_factories import': 'from src.api.factories import',
    r'from models\.response_factories import': 'from src.api.factories import',
    
    # Constants - special handling for PermissionType
    r'from \.\.models\.pydantic_models import PermissionType': 'from ..core.abstractions.models import PermissionType',
    r'from src\.models\.pydantic_models import PermissionType': 'from src.core.abstractions.models import PermissionType',
}

# Additional specific imports that need updating
SPECIFIC_IMPORTS = {
    'CurrentUser': 'src.api.models',
    'PermissionType': 'src.core.abstractions.models',
    'JobStatus': 'src.core.abstractions.models',
    'ImportStatus': 'src.core.abstractions.models',
    'PermissionLevel': 'src.core.abstractions.models',
}


def update_imports_in_file(file_path: Path) -> bool:
    """Update imports in a single file."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        original_content = content
        
        # Apply general import mappings
        for pattern, replacement in IMPORT_MAPPINGS.items():
            content = re.sub(pattern, replacement, content)
        
        # Handle multiline imports
        # Example: from ..models.pydantic_models import (\n    CurrentUser,\n    PermissionType\n)
        multiline_pattern = r'from\s+(?:\.\.|src\.)?models\.pydantic_models\s+import\s+\([^)]+\)'
        multiline_matches = re.findall(multiline_pattern, content, re.MULTILINE | re.DOTALL)
        
        for match in multiline_matches:
            # Extract imported items
            imports_match = re.search(r'import\s+\(([^)]+)\)', match, re.MULTILINE | re.DOTALL)
            if imports_match:
                imports_str = imports_match.group(1)
                import_items = [item.strip() for item in imports_str.split(',') if item.strip()]
                
                # Separate items by their target module
                api_models = []
                abstractions = []
                
                for item in import_items:
                    # Clean the item (remove comments, etc.)
                    clean_item = item.split('#')[0].strip()
                    if clean_item in ['PermissionType', 'JobStatus', 'ImportStatus', 'PermissionLevel']:
                        abstractions.append(clean_item)
                    else:
                        api_models.append(clean_item)
                
                # Build replacement imports
                replacement_imports = []
                if api_models:
                    if len(api_models) == 1:
                        replacement_imports.append(f"from ..api.models import {api_models[0]}")
                    else:
                        items_str = ',\n    '.join(api_models)
                        replacement_imports.append(f"from ..api.models import (\n    {items_str}\n)")
                
                if abstractions:
                    if len(abstractions) == 1:
                        replacement_imports.append(f"from ..core.abstractions.models import {abstractions[0]}")
                    else:
                        items_str = ',\n    '.join(abstractions)
                        replacement_imports.append(f"from ..core.abstractions.models import (\n    {items_str}\n)")
                
                # Replace the original import
                if replacement_imports:
                    content = content.replace(match, '\n'.join(replacement_imports))
        
        # Write back if changed
        if content != original_content:
            with open(file_path, 'w') as f:
                f.write(content)
            return True
        
        return False
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False


def main():
    """Update all Python files in the src directory."""
    src_dir = Path(__file__).parent.parent / 'src'
    
    updated_files = []
    
    for py_file in src_dir.rglob('*.py'):
        # Skip the models directory itself and __pycache__
        if 'models' in py_file.parts and py_file.parent.name == 'models':
            continue
        if '__pycache__' in str(py_file):
            continue
        
        if update_imports_in_file(py_file):
            updated_files.append(py_file)
    
    print(f"Updated {len(updated_files)} files:")
    for f in sorted(updated_files):
        print(f"  - {f.relative_to(src_dir.parent)}")


if __name__ == '__main__':
    main()