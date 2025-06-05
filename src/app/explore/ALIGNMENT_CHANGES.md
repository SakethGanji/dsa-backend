# Exploration Module Alignment with Sampling Module

## Summary of Changes

This document outlines the changes made to align the exploration module with the sampling module's approach for dataset pulling.

## Key Differences Addressed

1. **Storage Type Support**: 
   - Previously: Only supported database storage (file_data as bytes)
   - Now: Supports both database and filesystem storage

2. **Parquet File Handling**:
   - Previously: No special handling for Parquet files
   - Now: Uses DuckDB for efficient Parquet file processing

3. **Memory Management**:
   - Previously: Always loaded entire dataset into pandas DataFrame
   - Now: For Parquet files on filesystem, uses DuckDB views for metadata extraction without loading all data

## Implementation Changes

### 1. Import Changes
Added DuckDB import to support efficient Parquet file handling:
```python
import duckdb
```

### 2. Enhanced Validation (`_validate_and_get_data`)
- Now checks storage type (filesystem vs database)
- Validates appropriate data availability based on storage type
- Raises specific errors for missing file paths or file data

### 3. Updated DataFrame Loading (`_load_dataframe`)
The method now handles multiple scenarios:
- **Parquet on filesystem**: Uses DuckDB with memory limits (2GB) for efficient loading
- **Database storage**: Uses BytesIO for in-memory processing
- **Filesystem CSV/Excel**: Reads directly from file path
- Provides better error handling with detailed error messages

### 4. Smart Data Loading (`explore_dataset`)
- For Parquet files on filesystem:
  - First gets metadata using DuckDB without loading all data
  - Only loads full data if profiling is requested
  - Returns lightweight summary for non-profiling requests
- For other files: Uses traditional approach

### 5. New Method: `_get_parquet_summary_with_duckdb`
Efficiently extracts metadata from Parquet files:
- Row count from Parquet metadata
- Column names and types
- Sample data (first 10 rows)
- Memory usage estimation
- All without loading the entire dataset

### 6. Enhanced Response Creation
`_create_response` now accepts an optional precomputed summary to avoid redundant calculations.

## Benefits

1. **Performance**: Large Parquet files are no longer fully loaded unless necessary
2. **Memory Efficiency**: Uses DuckDB views instead of loading entire datasets
3. **Flexibility**: Supports both database and filesystem storage
4. **Compatibility**: Maintains backward compatibility with existing CSV/Excel handling

## Usage Examples

### Quick Summary (No Full Loading)
For Parquet files on filesystem with `run_profiling=false`:
```json
{
  "summary": {
    "rows": 1000000,
    "columns": 50,
    "column_names": [...],
    "column_types": {...},
    "memory_usage_mb": 381.47,
    "sample": [...]
  },
  "format": "json",
  "message": "Summary generated using DuckDB. Set run_profiling=true for full profiling."
}
```

### Full Profiling
For any file type with `run_profiling=true`:
- Loads data (with limits for Parquet)
- Generates full ydata-profiling report
- Returns complete analysis

## Migration Notes

- No changes required to existing API contracts
- Existing database-stored files continue to work as before
- Parquet files on filesystem now benefit from optimized handling automatically