# Downloads Service Consolidation Plan

## Current State Analysis

Currently, the downloads functionality is split across 2 separate handler files:
1. `DownloadDatasetHandler` - Downloads entire datasets in various formats (csv, excel, json, parquet)
2. `DownloadTableHandler` - Downloads individual tables from datasets (csv, json)

### Key Observations:
- Both handlers follow similar patterns for validation and data retrieval
- Both depend on `PostgresUnitOfWork` 
- `DownloadDatasetHandler` uses external `DataExportService` for heavy lifting
- `DownloadTableHandler` handles its own formatting logic
- Both return similar response structures with content, content_type, and filename
- Less complex than other features (only 2 handlers)

## Proposed Solution: Consolidated DownloadService

### Benefits:
1. **Unified interface** - Single service for all download operations
2. **Consistent formatting** - Centralized format handling logic
3. **Better resource management** - Single point for managing table readers
4. **Easier testing** - One service to test
5. **Consistent patterns** - Matches DatasetService, SamplingService, and ExplorationService approach

### Proposed Structure:

```python
# src/features/downloads/services/download_service.py

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import io
import csv
import json

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.table_reader import PostgresTableReader
from src.services import DataExportService, ExportOptions
from src.core.domain_exceptions import EntityNotFoundException, ValidationException
from ...base_handler import with_error_handling
from ..models import DownloadDatasetCommand, DownloadTableCommand


@dataclass
class DownloadResponse:
    """Response for download operations."""
    content: bytes
    content_type: str
    filename: str
    metadata: Optional[Dict[str, Any]] = None


class DownloadService:
    """Consolidated service for all download operations."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        table_reader: PostgresTableReader,
        export_service: Optional[DataExportService] = None
    ):
        self._uow = uow
        self._table_reader = table_reader
        self._export_service = export_service
    
    @with_error_handling
    async def download_dataset(
        self,
        command: DownloadDatasetCommand
    ) -> DownloadResponse:
        """Download entire dataset in specified format."""
        # Validate format
        valid_formats = ["csv", "excel", "json", "parquet"]
        if command.format not in valid_formats:
            raise ValidationException(
                f"Unsupported format: {command.format}. Valid formats: {valid_formats}",
                field="format"
            )
        
        # Implementation from DownloadDatasetHandler
        # ...
        
    @with_error_handling
    async def download_table(
        self,
        command: DownloadTableCommand
    ) -> DownloadResponse:
        """Download a specific table in specified format."""
        # Validate format
        if command.format not in ["csv", "json"]:
            raise ValidationException(f"Unsupported format: {command.format}", field="format")
        
        # Implementation from DownloadTableHandler
        # ...
        
    # Private helper methods for formatting
    async def _format_as_csv(
        self, 
        data: List[Dict], 
        dataset_name: str,
        table_key: str,
        commit_id: str
    ) -> bytes:
        """Format data as CSV."""
        # Shared CSV formatting logic
        pass
    
    async def _format_as_json(
        self,
        data: List[Dict],
        dataset_name: str,
        table_key: str,
        commit_id: str,
        include_schema: bool = True
    ) -> bytes:
        """Format data as JSON with optional schema."""
        # Shared JSON formatting logic
        pass
```

## Implementation Steps

### 1. Create Service Structure
```bash
mkdir -p src/features/downloads/services
touch src/features/downloads/services/__init__.py
touch src/features/downloads/services/download_service.py
```

### 2. Migrate Handler Logic
- Copy logic from both handlers into corresponding service methods
- Extract common formatting logic into shared private methods
- Maintain the same validation and error handling patterns

### 3. Update API Endpoints
Transform each endpoint from:
```python
handler = DownloadDatasetHandler(uow, export_service)
return await handler.handle(command)
```

To:
```python
service = DownloadService(uow, table_reader, export_service)
return await service.download_dataset(command)
```

### 4. Update Dependencies
The service will need access to:
- `PostgresUnitOfWork` 
- `PostgresTableReader`
- `DataExportService` (optional, for dataset downloads)

### 5. Clean Up
- Remove old handler files
- Update imports throughout codebase
- Update module exports

## Special Considerations

### 1. External Service Dependencies
- `DataExportService` is only needed for full dataset downloads
- Make it optional in the service constructor
- Create it on-demand if not provided

### 2. Table Reader Management
- Both handlers currently use table readers
- Consolidate to single table reader instance
- Pass it from API layer for consistency

### 3. Response Format
- Standardize response with `DownloadResponse` dataclass
- Keep consistent structure for both download types
- Include optional metadata field for extensibility

### 4. Streaming vs In-Memory
- Current handlers return full content in memory
- Consider future enhancement for streaming large datasets
- Keep current approach for MVP

## Migration Checklist

- [ ] Create services directory structure
- [ ] Create DownloadService class
- [ ] Migrate download_dataset logic
- [ ] Migrate download_table logic
- [ ] Extract common formatting methods
- [ ] Update API endpoints in `src/api/downloads.py`
- [ ] Remove old handler files
- [ ] Update handler exports in `handlers/__init__.py`
- [ ] Test both download endpoints
- [ ] Verify file formats are correct

## Common Pitfalls to Avoid

1. **Service Dependencies**: Ensure DataExportService is properly initialized
2. **Table Reader**: Pass the same table reader instance to avoid connection issues
3. **Memory Usage**: Be aware that current approach loads all data in memory
4. **Format Validation**: Keep validation consistent between methods

## Expected Outcome

After consolidation:
- Single `DownloadService` class with 2 main methods
- Cleaner API endpoints
- Shared formatting logic reduces duplication
- Better testability
- Consistent with other service patterns
- No functional changes - API remains the same

## Testing Commands

After implementation:
```bash
# Check Python syntax
python3 -m py_compile src/api/downloads.py src/features/downloads/services/download_service.py

# Run server
python3 -m uvicorn src.main:app --reload

# Test download endpoints
curl http://localhost:8000/datasets/1/refs/main/download?format=csv
curl http://localhost:8000/datasets/1/refs/main/tables/primary/download?format=json
```

## Future Enhancements

1. **Streaming Downloads**: Implement streaming for large datasets
2. **Compression**: Add gzip compression option
3. **Batch Downloads**: Support downloading multiple tables in a zip
4. **Format Plugins**: Make format handlers pluggable
5. **Progress Tracking**: Add download progress for large exports