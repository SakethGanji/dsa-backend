[# UI Guide: Table Analysis Endpoint

## Endpoint Overview

**GET** `/api/datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/analysis`

This endpoint provides comprehensive analysis data for a specific table within a dataset reference. It combines pre-calculated statistics with real-time sampling to provide rich metadata for UI display.

## When Analysis is Calculated

### During Import (Pre-calculated)
- **Timing**: Statistics are calculated automatically during the file import/upload process
- **Location**: Calculated in `ProcessImportJobHandler._parse_file` (src/features/versioning/process_import_job.py:119)
- **What's calculated**: Row counts, null counts, and other statistical metadata
- **Storage**: Stored in the `commit_statistics` table when the commit is created

### During API Call (On-demand)
- **Sample values**: Up to 1000 rows are sampled to extract unique values
- **Column type inference**: If types weren't determined during import
- **Data combination**: Pre-calculated stats are merged with sampled data

## Request Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| dataset_id | UUID | Yes | The unique identifier of the dataset |
| ref_name | string | Yes | The reference name (e.g., "main", "dev") |
| table_key | string | Yes | The table identifier within the dataset |

## Response Structure

```json
{
  "table_key": "string",
  "columns": ["column1", "column2", ...],
  "column_types": {
    "column1": "string",
    "column2": "integer",
    "column3": "float"
  },
  "total_rows": 1000,
  "null_counts": {
    "column1": 5,
    "column2": 0,
    "column3": 23
  },
  "sample_values": {
    "column1": ["value1", "value2", ...],  // Up to 20 unique values
    "column2": [1, 2, 3, ...],
    "column3": [1.5, 2.7, 3.9, ...]
  },
  "statistics": {
    // Additional statistics from commit_statistics table
  }
}
```

## Error Handling

### Common Error Scenarios
1. **404 Not Found**: Table doesn't exist in the specified ref
2. **403 Forbidden**: User lacks permission to view the dataset
3. **500 Internal Error**: Statistics calculation failed

### Error Response Format
```json
{
  "error": "Table not found",
  "code": "TABLE_NOT_FOUND",
  "details": "No table with key 'customers' found in ref 'main'"
}
```