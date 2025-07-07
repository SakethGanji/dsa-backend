# Sampling API Reference

## Overview

The DSA platform provides a comprehensive set of sampling endpoints for data exploration and analysis. This document describes each endpoint's purpose, request format, and response structure.

## Endpoints

### 1. Get Available Sampling Methods

Returns all supported sampling methods and their parameters.

**Endpoint:** `GET /api/sampling/datasets/{dataset_id}/sampling-methods`

**Path Parameters:**
- `dataset_id` (integer): The dataset ID

**Headers:**
- `Authorization: Bearer {token}`

**Response:**
```json
{
  "methods": [
    {
      "name": "random",
      "description": "Simple random sampling with optional seed for reproducibility",
      "parameters": [
        {
          "name": "sample_size",
          "type": "integer",
          "required": true,
          "description": "Number of samples"
        },
        {
          "name": "seed",
          "type": "integer",
          "required": false,
          "description": "Random seed for reproducibility"
        }
      ]
    },
    {
      "name": "stratified",
      "description": "Stratified sampling ensuring representation from all strata",
      "parameters": [
        {
          "name": "sample_size",
          "type": "integer",
          "required": true,
          "description": "Number of samples"
        },
        {
          "name": "strata_columns",
          "type": "array",
          "required": true,
          "description": "Columns to stratify by"
        },
        {
          "name": "proportional",
          "type": "boolean",
          "required": false,
          "description": "Use proportional allocation"
        },
        {
          "name": "min_per_stratum",
          "type": "integer",
          "required": false,
          "description": "Minimum samples per stratum"
        }
      ]
    },
    {
      "name": "systematic",
      "description": "Systematic sampling with fixed intervals",
      "parameters": [
        {
          "name": "sample_size",
          "type": "integer",
          "required": true,
          "description": "Number of samples"
        },
        {
          "name": "interval",
          "type": "integer",
          "required": true,
          "description": "Sampling interval"
        },
        {
          "name": "start",
          "type": "integer",
          "required": false,
          "description": "Starting position"
        }
      ]
    },
    {
      "name": "cluster",
      "description": "Cluster sampling selecting entire groups",
      "parameters": [
        {
          "name": "sample_size",
          "type": "integer",
          "required": true,
          "description": "Number of samples"
        },
        {
          "name": "cluster_column",
          "type": "string",
          "required": true,
          "description": "Column defining clusters"
        },
        {
          "name": "num_clusters",
          "type": "integer",
          "required": true,
          "description": "Number of clusters to select"
        }
      ]
    }
  ],
  "supported_operators": [
    ">", ">=", "<", "<=", "=", "!=", "in", "not_in", 
    "like", "ilike", "is_null", "is_not_null"
  ]
}
```

**Use Case:** Display available sampling methods in UI, understand parameter requirements

---

### 2. Get Column Sample Values

Returns unique sample values for specified columns. Optimized for building filter dropdowns.

**Endpoint:** `POST /api/sampling/datasets/{dataset_id}/column-samples`

**Path Parameters:**
- `dataset_id` (integer): The dataset ID

**Headers:**
- `Authorization: Bearer {token}`
- `Content-Type: application/json`

**Request Body:**
```json
{
  "columns": ["column1", "column2", "SheetName:column3"],  // For multi-sheet files
  "sample_size": 20,  // Optional, default: 20, max unique values per column
  "ref": "main"  // Optional, default: "main"
}
```

**Response:**
```json
{
  "samples": {
    "column1": ["value1", "value2", "value3"],
    "column2": ["valueA", "valueB"],
    "SheetName:column3": ["val1", "val2", "val3", "val4"]
  },
  "metadata": {
    "dataset_id": 123,
    "ref_name": "main",
    "table_key": "primary",
    "commit_id": "20240101120000_abc123",
    "columns_requested": 3,
    "samples_per_column": 20
  }
}
```

**Notes:**
- Uses `TABLESAMPLE SYSTEM(10%)` for performance
- Returns up to `sample_size` unique values per column
- For multi-sheet Excel files, use format: `SheetName:columnName`

**Use Case:** Populate filter dropdowns, show possible column values

---

### 3. Sample Data (Synchronous)

Performs immediate sampling and returns results. Best for small to medium datasets.

**Endpoint:** `POST /api/sampling/datasets/{dataset_id}/sample`

**Path Parameters:**
- `dataset_id` (integer): The dataset ID

**Headers:**
- `Authorization: Bearer {token}`
- `Content-Type: application/json`

**Request Body:**
```json
{
  "method": "stratified",  // Required: random, stratified, systematic, cluster
  "sample_size": 100,      // Required: number of rows to sample
  "parameters": {          // Method-specific parameters
    "strata_columns": ["region", "category"],
    "proportional": true,
    "min_per_stratum": 5
  },
  "sheets": ["Sales", "Inventory"],  // Optional: specific sheets for Excel files
  "ref": "main",                     // Optional: default "main"
  "filters": [                       // Optional: pre-filter data
    {
      "column": "region",
      "operator": "in",
      "value": ["North", "South"]
    },
    {
      "column": "date",
      "operator": ">=",
      "value": "2024-01-01"
    }
  ],
  "offset": 0,    // Optional: for pagination
  "limit": 100    // Optional: max rows to return
}
```

**Response:**
```json
{
  "data": [
    {
      "row_id": "Sales:0",
      "date": "2024-01-15",
      "region": "North",
      "sales": 1500.50,
      "category": "Electronics"
    },
    // ... more rows
  ],
  "metadata": {
    "commit_id": "20240101120000_abc123",
    "table_key": "Sales",
    "sampling_params": {
      "method": "stratified",
      "sample_size": 100,
      "parameters": {
        "strata_columns": ["region", "category"],
        "proportional": true
      }
    },
    "actual_sample_size": 100,
    "total_sampled": 100,
    "offset": 0,
    "limit": 100,
    "returned": 100
  },
  "method": "stratified",
  "sample_size": 100,
  "strata_counts": {
    "North,Electronics": 25,
    "North,Clothing": 20,
    "South,Electronics": 30,
    "South,Clothing": 25
  }
}
```

**Use Case:** Quick data preview, interactive sampling, small dataset sampling

---

### 4. Create Sampling Job (Asynchronous)

Creates a background job for large sampling operations with optional output commit.

**Endpoint:** `POST /api/sampling/datasets/{dataset_id}/jobs`

**Path Parameters:**
- `dataset_id` (integer): The dataset ID

**Headers:**
- `Authorization: Bearer {token}`
- `Content-Type: application/json`

**Request Body:**
```json
{
  "source_ref": "main",           // Optional: default "main"
  "table_key": "Sales",           // Optional: default "primary", for Excel use sheet name
  "create_output_commit": true,   // Optional: default true
  "commit_message": "Stratified sample of Q1 2024 sales data",
  "rounds": [                     // Required: array of sampling rounds
    {
      "round_number": 1,
      "method": "stratified",
      "sample_size": 10000,
      "parameters": {
        "strata_columns": ["region", "product_category"],
        "proportional": true,
        "min_per_stratum": 50
      },
      "filters": [
        {
          "column": "date",
          "operator": "between",
          "value": ["2024-01-01", "2024-03-31"]
        }
      ],
      "exclude_previous_rounds": true
    },
    {
      "round_number": 2,
      "method": "random",
      "sample_size": 5000,
      "parameters": {
        "seed": 42
      },
      "filters": [
        {
          "column": "sales_amount",
          "operator": ">",
          "value": 1000
        }
      ]
    }
  ],
  "export_residual": true,              // Optional: also save unsampled records
  "residual_output_name": "unsampled"   // Optional: name for residual data
}
```

**Response:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "message": "Sampling job created successfully"
}
```

**Job Status (via GET /api/jobs/{job_id}):**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",  // pending, running, completed, failed
  "progress": 100,
  "created_at": "2024-01-15T10:30:00Z",
  "completed_at": "2024-01-15T10:31:45Z",
  "output_summary": {
    "rounds_completed": 2,
    "total_sampled": 15000,
    "output_commit_id": "20240115103145_def456",
    "residual_rows": 85000
  }
}
```

**Use Case:** Large dataset sampling, multi-round sampling, creating permanent samples

---

### 5. Get Sampling Job Data

Retrieves the sampled data from a completed job.

**Endpoint:** `GET /api/sampling/jobs/{job_id}/data`

**Path Parameters:**
- `job_id` (uuid): The sampling job ID

**Headers:**
- `Authorization: Bearer {token}`

**Query Parameters:**
- `offset` (integer): Optional, default 0
- `limit` (integer): Optional, default 1000, max 10000

**Response:**
```json
{
  "data": [
    {
      "round": 1,
      "row_id": "Sales:42",
      "date": "2024-01-15",
      "region": "North",
      "product_category": "Electronics",
      "sales_amount": 2500.00
    },
    // ... more rows
  ],
  "metadata": {
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "total_rows": 15000,
    "offset": 0,
    "limit": 1000,
    "returned": 1000
  }
}
```

**Use Case:** Retrieve results after job completion

---

### 6. Get Sampling Job Residual Data

Retrieves unsampled (residual) data from a job with `export_residual: true`.

**Endpoint:** `GET /api/sampling/jobs/{job_id}/residual`

**Path Parameters:**
- `job_id` (uuid): The sampling job ID

**Headers:**
- `Authorization: Bearer {token}`

**Query Parameters:**
- `offset` (integer): Optional, default 0
- `limit` (integer): Optional, default 1000, max 10000

**Response:**
```json
{
  "data": [
    {
      "row_id": "Sales:1001",
      "date": "2024-02-20",
      "region": "West",
      "product_category": "Clothing",
      "sales_amount": 500.00
    },
    // ... more unsampled rows
  ],
  "metadata": {
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "total_residual_rows": 85000,
    "offset": 0,
    "limit": 1000,
    "returned": 1000
  }
}
```

**Use Case:** Access data not included in sample for validation or separate analysis

---

### 7. Get Dataset Sampling History

Returns all sampling jobs for a specific dataset.

**Endpoint:** `GET /api/sampling/datasets/{dataset_id}/history`

**Path Parameters:**
- `dataset_id` (integer): The dataset ID

**Headers:**
- `Authorization: Bearer {token}`

**Query Parameters:**
- `limit` (integer): Optional, default 50
- `offset` (integer): Optional, default 0

**Response:**
```json
{
  "jobs": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "dataset_id": 123,
      "user_id": 456,
      "user_name": "john.doe",
      "status": "completed",
      "created_at": "2024-01-15T10:30:00Z",
      "completed_at": "2024-01-15T10:31:45Z",
      "parameters": {
        "source_ref": "main",
        "table_key": "Sales",
        "rounds": [
          {
            "round_number": 1,
            "method": "stratified",
            "sample_size": 10000
          }
        ]
      },
      "output_summary": {
        "total_sampled": 10000,
        "output_commit_id": "20240115103145_def456"
      }
    }
    // ... more jobs
  ],
  "total": 25,
  "limit": 50,
  "offset": 0
}
```

**Use Case:** View sampling history, reuse previous configurations, audit trail

---

### 8. Get User Sampling History

Returns all sampling jobs created by a specific user.

**Endpoint:** `GET /api/sampling/users/{user_id}/history`

**Path Parameters:**
- `user_id` (integer): The user ID

**Headers:**
- `Authorization: Bearer {token}`

**Query Parameters:**
- `limit` (integer): Optional, default 50
- `offset` (integer): Optional, default 0
- `dataset_id` (integer): Optional, filter by dataset

**Response:**
```json
{
  "jobs": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "dataset_id": 123,
      "dataset_name": "Q1 Sales Data",
      "status": "completed",
      "created_at": "2024-01-15T10:30:00Z",
      "parameters": {
        "table_key": "Sales",
        "rounds": [
          {
            "method": "random",
            "sample_size": 1000
          }
        ]
      }
    }
    // ... more jobs
  ],
  "total": 150,
  "limit": 50,
  "offset": 0
}
```

**Use Case:** User activity tracking, personal sampling history

---

## Common Response Codes

- `200 OK`: Successful request
- `202 Accepted`: Job created successfully (async operations)
- `400 Bad Request`: Invalid parameters (e.g., sample size too large)
- `401 Unauthorized`: Invalid or missing token
- `404 Not Found`: Dataset or job not found
- `422 Unprocessable Entity`: Validation error (e.g., invalid column name)
- `500 Internal Server Error`: Server error

## Notes for Multi-Sheet Excel Files

When working with multi-sheet Excel files:
- Use sheet names as `table_key` in job requests
- Reference columns as `SheetName:columnName` in filters and column samples
- The schema endpoint will show all available sheets and their columns

## Rate Limiting

- Synchronous sampling (`/sample`): Limited by response size and query complexity
- Column samples: Lightweight, suitable for frequent calls
- Job creation: No specific limit, but jobs are queued
- History endpoints: Standard pagination limits apply