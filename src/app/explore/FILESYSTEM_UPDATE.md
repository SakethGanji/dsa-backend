# Exploration Module - Filesystem Update

## Overview

The exploration module has been updated to align with the sampling module's approach for pulling datasets from the filesystem instead of the database.

## Changes Made

### 1. **Updated `_validate_and_get_data` method**
- Removed checks for database storage type
- Now only validates that `file_path` exists
- Added file size check (max 50GB) similar to sampling module
- Simplified error handling for missing file paths

### 2. **Simplified `_load_dataframe` method**
- Removed all database-related code (BytesIO, file_data)
- Now assumes all files are stored on filesystem
- Maintains support for:
  - Parquet files (using DuckDB for efficiency)
  - CSV files (using pandas)
  - Excel files (using pandas with sheet support)

### 3. **Updated `explore_dataset` method**
- Removed storage type checks
- Simplified logic since all files are now on filesystem
- Maintains efficient handling for large Parquet files

## Key Benefits

1. **Consistency**: Aligned with sampling module's approach
2. **Performance**: DuckDB integration for efficient Parquet handling
3. **Simplicity**: Removed complex storage type branching logic
4. **Memory Efficiency**: Large files handled without full loading

## Usage

The API remains unchanged. The exploration endpoint continues to work as before:

```json
POST /api/explore/{dataset_id}/{version_id}
{
  "format": "json",
  "run_profiling": false,
  "sheet": null
}
```

## File Type Support

- **Parquet**: Uses DuckDB for metadata extraction and limited sampling
- **CSV**: Direct pandas loading from file path
- **Excel**: Direct pandas loading with optional sheet selection

## Migration Notes

- Ensure all datasets have been migrated to filesystem storage
- Database file_data column is no longer used
- All files must have valid file_path entries