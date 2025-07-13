#!/bin/bash
# Script to update all pagination imports to use the unified module

echo "Starting pagination import updates..."

# Find all Python files that import PaginationMixin
FILES=$(find ./src -name "*.py" -type f | xargs grep -l "from src.api.common import PaginationMixin\|from src.features.base_handler import.*PaginationMixin" 2>/dev/null || true)

if [ -z "$FILES" ]; then
    echo "No files found with old pagination imports"
    exit 0
fi

# Counter for processed files
count=0

for file in $FILES; do
    echo "Processing: $file"
    
    # Create backup
    cp "$file" "${file}.bak"
    
    # Check which pattern to use
    if grep -q "from src.features.base_handler import.*PaginationMixin" "$file"; then
        # Handle case: from src.features.base_handler import BaseHandler, PaginationMixin
        sed -i 's/from src.features.base_handler import BaseHandler, PaginationMixin/from src.features.base_handler import BaseHandler\nfrom src.core.common.pagination import PaginationMixin/g' "$file"
        sed -i 's/from src.features.base_handler import PaginationMixin, BaseHandler/from src.features.base_handler import BaseHandler\nfrom src.core.common.pagination import PaginationMixin/g' "$file"
    fi
    
    # Handle case: from src.api.common import PaginationMixin
    sed -i 's/from src.api.common import PaginationMixin/from src.core.common.pagination import PaginationMixin/g' "$file"
    
    # Remove duplicate imports if any
    awk '!seen[$0]++' "$file" > "${file}.tmp" && mv "${file}.tmp" "$file"
    
    count=$((count + 1))
    echo "✓ Updated $file"
done

echo ""
echo "Summary:"
echo "- Updated $count files"
echo "- Backup files created with .bak extension"
echo ""
echo "To verify changes:"
echo "  grep -r 'from src.core.common.pagination import PaginationMixin' ./src"
echo ""
echo "To remove backups after verification:"
echo "  find ./src -name '*.bak' -delete"