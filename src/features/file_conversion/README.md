# File Conversion Service

Unified service for converting between files and database data.

## Usage Examples

### Import (File → Database)
```python
from src.features.file_conversion.services.file_conversion_service import FileConversionService

# Create service
service = FileConversionService(uow)

# Import a file
parsed_data = await service.import_file("/path/to/data.csv", "data.csv")

# Access parsed tables
for table in parsed_data.tables:
    print(f"Table: {table.table_key}")
    print(f"Data shape: {table.dataframe.shape}")
```

### Export (Database → File)
```python
from src.features.file_conversion.models.file_format import FileFormat, ConversionOptions

# Export to CSV
result = await service.export_data(
    dataset_id=123,
    commit_id="abc123",
    table_name="primary",
    format=FileFormat.CSV,
    options=ConversionOptions(
        columns=["id", "name", "value"],
        include_headers=True
    )
)

# result.file_path contains the exported file
```

## Supported Formats
- Import: CSV, Excel (.xlsx, .xls), Parquet
- Export: CSV, Excel, JSON, Parquet