# Datasets Slice

This vertical slice handles all dataset-related operations including upload, versioning, and data retrieval.

## Architecture

The datasets slice follows vertical slice architecture with clear separation of concerns:

### Layer Structure

```
datasets/
├── models.py          # Pydantic models for request/response
├── routes.py          # FastAPI route definitions  
├── controller.py      # HTTP request/response handling
├── service.py         # Business logic layer
├── repository.py      # Data access layer
├── exceptions.py      # Custom exceptions
├── validators.py      # Input validation logic
├── constants.py       # Configuration constants
├── file_parsers.py    # File parsing strategies
├── query_builder.py   # SQL query construction
├── formatters.py      # Response formatting
├── duckdb_service.py  # DuckDB integration
└── search/            # Search functionality module
    ├── models.py      # Search request/response models
    ├── repository.py  # Search database operations
    ├── service.py     # Search business logic
    └── routes.py      # Search API endpoints
```

### Key Design Patterns

1. **Dependency Injection**: Clean dependency management through FastAPI's DI system
2. **Repository Pattern**: Abstracts database operations 
3. **Service Layer**: Contains all business logic
4. **Strategy Pattern**: File parsers for different formats
5. **Builder Pattern**: Query builder for complex filters

## Features

- **File Upload**: Support for CSV, Excel (xlsx, xls, xlsm) files
- **Versioning**: Automatic version management for datasets
- **Tagging**: Flexible tagging system for organization
- **Filtering**: Advanced filtering with multiple criteria
- **Pagination**: Efficient data retrieval for large datasets
- **Storage**: Converts all files to Parquet for optimal performance
- **Full-Text Search**: PostgreSQL-based search with relevance ranking
- **Fuzzy Search**: Typo-tolerant search using pg_trgm extension
- **Faceted Search**: Aggregated counts for common filter values
- **Autocomplete**: Real-time search suggestions

## API Endpoints

### Upload Dataset
```
POST /api/datasets/upload
```
Upload a new dataset or create a new version.

### List Datasets
```
GET /api/datasets
```
Retrieve datasets with filtering, sorting, and pagination.

### Get Dataset
```
GET /api/datasets/{dataset_id}
```
Get detailed information about a specific dataset.

### Update Dataset
```
PATCH /api/datasets/{dataset_id}
```
Update dataset metadata (name, description, tags).

### List Versions
```
GET /api/datasets/{dataset_id}/versions
```
Get all versions of a dataset.

### Download Version
```
GET /api/datasets/{dataset_id}/versions/{version_id}/download
```
Download the raw file for a specific version.

### Get Sheet Data
```
GET /api/datasets/{dataset_id}/versions/{version_id}/data
```
Retrieve paginated data from a dataset sheet.

### Search Datasets
```
POST /api/datasets/search
GET /api/datasets/search
```
Advanced search with full-text search, fuzzy matching, and faceted results. See [Search API Documentation](search/SEARCH_API_DOCUMENTATION.md) for details.

### Search Suggestions
```
POST /api/datasets/search/suggest
GET /api/datasets/search/suggest
```
Get autocomplete suggestions for search queries.

## Error Handling

Custom exceptions provide clear error messages:

- `DatasetNotFound`: Dataset does not exist
- `DatasetVersionNotFound`: Version does not exist
- `FileProcessingError`: File parsing failed
- `StorageError`: Storage operation failed
- `SheetNotFound`: Requested sheet not found

## Configuration

Key constants in `constants.py`:

- `MAX_FILE_SIZE`: 500MB maximum file size
- `MAX_ROWS_PER_PAGE`: 1000 rows per page maximum
- `DEFAULT_PAGE_SIZE`: 100 rows default page size
- `MAX_TAGS_PER_DATASET`: 20 tags maximum
- `MAX_TAG_LENGTH`: 50 characters per tag

## Storage

All uploaded files are converted to Parquet format for:
- Optimal query performance with DuckDB
- Efficient storage compression
- Consistent data format

Files are stored in:
```
data/datasets/{dataset_id}/{version_id}/
```

## Testing

The slice includes comprehensive validation:
- File type and size validation
- Tag format validation  
- Pagination parameter validation
- Permission checks via auth dependency

## Future Enhancements

1. **Caching**: Add Redis caching for frequently accessed data
2. **Async Processing**: Background jobs for large file processing
3. **Data Profiling**: Automatic data quality reports
4. **Export Formats**: Support multiple export formats
5. **Webhooks**: Notifications for dataset updates