# Columns Endpoint Design Document

## Overview

This document outlines the design for a dedicated columns endpoint to support UI functionality in the DSA (Data Storage Abstraction) platform. The endpoint will provide lightweight, efficient access to column metadata for UI components such as column selectors, data grids, and filtering interfaces.

## Background

Currently, column information is available through:
- **Table Schema Endpoint**: Returns full schema including columns with types
- **Table Analysis Endpoint**: Returns comprehensive analysis including columns, types, null counts, and sample values

However, UI components often need a lightweight, fast endpoint specifically for column metadata without the overhead of full schema or analysis data.

## Proposed Endpoint

### Basic Column List Endpoint
```
GET /datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/columns
```

#### Response Model
```python
class ColumnInfo(BaseModel):
    name: str
    type: str
    nullable: bool = True
    
class ColumnsResponse(BaseModel):
    columns: List[ColumnInfo]
    total_count: int
```

#### Example Response
```json
{
  "columns": [
    {
      "name": "user_id",
      "type": "integer",
      "nullable": false
    },
    {
      "name": "created_at",
      "type": "timestamp",
      "nullable": false
    },
    {
      "name": "email",
      "type": "string",
      "nullable": true
    }
  ],
  "total_count": 3
}
```

## Extended Features

### 1. Column Metadata with Statistics (Optional)
```
GET /datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/columns?include_stats=true
```

#### Extended Response Model
```python
class ColumnStats(BaseModel):
    null_count: Optional[int] = None
    unique_count: Optional[int] = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    
class ExtendedColumnInfo(ColumnInfo):
    stats: Optional[ColumnStats] = None
    sample_values: Optional[List[Any]] = None
```

### 2. Column Search and Filtering
```
GET /datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/columns?search=user&type=string
```

Query Parameters:
- `search`: Partial match on column name
- `type`: Filter by data type
- `limit`: Maximum number of columns to return
- `offset`: Pagination offset

### 3. Bulk Column Operations
```
POST /datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/columns/bulk
```

Request Body:
```json
{
  "column_names": ["user_id", "email", "created_at"],
  "include_stats": true
}

## API Examples

### Basic Column List
```bash
curl -X GET "https://api.dsa.com/datasets/123/refs/main/tables/users/columns" \
  -H "Authorization: Bearer ${TOKEN}"
```

### Columns with Statistics
```bash
curl -X GET "https://api.dsa.com/datasets/123/refs/main/tables/users/columns?include_stats=true" \
  -H "Authorization: Bearer ${TOKEN}"
```

### Search Columns
```bash
curl -X GET "https://api.dsa.com/datasets/123/refs/main/tables/users/columns?search=email&type=string" \
  -H "Authorization: Bearer ${TOKEN}"
```

## Error Handling

Standard HTTP status codes:
- `200 OK`: Success
- `404 Not Found`: Dataset/ref/table not found
- `403 Forbidden`: Insufficient permissions
- `400 Bad Request`: Invalid parameters

Error Response Format:
```json
{
  "error": {
    "code": "INVALID_COLUMN_TYPE",
    "message": "Invalid column type filter: 'number'. Valid types are: integer, string, boolean, timestamp, date, float, json",
    "details": {}
  }
}
```


## Conclusion

The columns endpoint will provide a dedicated, efficient interface for UI components to access column metadata. By starting with a minimal implementation and progressively adding features, we can ensure performance while meeting evolving UI requirements.