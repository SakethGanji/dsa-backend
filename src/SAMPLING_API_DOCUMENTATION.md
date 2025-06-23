# Sampling API Documentation

This document provides comprehensive documentation for all sampling-related API endpoints, including request/response examples.

## Table of Contents
1. [Authentication](#authentication)
2. [Get Dataset Columns](#1-get-dataset-columns)
3. [Create Multi-Round Sampling Job](#2-create-multi-round-sampling-job)
4. [Get Multi-Round Job Status](#3-get-multi-round-job-status)
5. [Get Merged Sample Data](#4-get-merged-sample-data)
6. [Get Round Preview](#5-get-round-preview)
7. [Get Residual Preview](#6-get-residual-preview)
8. [Execute Multi-Round Sampling Synchronously](#7-execute-multi-round-sampling-synchronously)
9. [Get Samplings by User ID](#8-get-samplings-by-user-id)
10. [Get Samplings by Dataset Version ID](#9-get-samplings-by-dataset-version-id)
11. [Get Samplings by Dataset ID](#10-get-samplings-by-dataset-id)

## Authentication

All endpoints require JWT authentication. First, obtain a token:

### Request
```bash
curl -X POST http://localhost:8000/api/users/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=bg54677&password=string"
```

### Response
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer"
}
```

Use the `access_token` in the Authorization header for all subsequent requests:
```
Authorization: Bearer <access_token>
```

---

## 1. Get Dataset Columns

Get column information for a dataset version to understand the data structure before sampling.

### Endpoint
`GET /api/sampling/{dataset_id}/{version_id}/columns`

### Request
```bash
curl -X GET "http://localhost:8000/api/sampling/14/14/columns" \
  -H "Authorization: Bearer <token>"
```

### Response
```json
{
  "columns": ["product", "quantity", "price"],
  "column_types": {
    "product": "VARCHAR",
    "quantity": "BIGINT",
    "price": "DOUBLE"
  },
  "total_rows": 4,
  "null_counts": {
    "product": null,
    "quantity": null,
    "price": null
  },
  "sample_values": {
    "product": ["Laptop", "Mouse", "Keyboard", "Monitor"],
    "quantity": [10, 50, 30, 15],
    "price": [999.99, 25.99, 45.99, 299.99]
  }
}
```

---

## 2. Create Multi-Round Sampling Job

Create an asynchronous multi-round sampling job for progressive residual sampling.

### Endpoint
`POST /api/sampling/{dataset_id}/{version_id}/multi-round/run`

### Request
```bash
curl -X POST "http://localhost:8000/api/sampling/14/14/multi-round/run" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "rounds": [
      {
        "round_number": 1,
        "method": "random",
        "parameters": {
          "sample_size": 1000,
          "seed": 42
        },
        "output_name": "round_1_random"
      },
      {
        "round_number": 2,
        "method": "stratified",
        "parameters": {
          "strata_columns": ["category"],
          "sample_size": 500
        },
        "output_name": "round_2_stratified",
        "filters": {
          "conditions": [
            {
              "column": "price",
              "operator": ">",
              "value": 100
            }
          ],
          "logic": "AND"
        }
      }
    ],
    "export_residual": true,
    "residual_output_name": "final_residual"
  }'
```

### Response
```json
{
  "run_id": "28",
  "status": "pending",
  "message": "Multi-round sampling job created with 2 rounds",
  "total_rounds": 2,
  "completed_rounds": 0,
  "current_round": null,
  "round_results": [],
  "residual_uri": null,
  "residual_size": null,
  "residual_summary": null,
  "error_message": null,
  "created_at": "2025-06-22T16:08:36.269584",
  "started_at": null,
  "completed_at": null
}
```

### Sampling Methods and Parameters

#### Random Sampling
```json
{
  "method": "random",
  "parameters": {
    "sample_size": 1000,
    "seed": 42  // Optional
  }
}
```

#### Stratified Sampling
```json
{
  "method": "stratified",
  "parameters": {
    "strata_columns": ["category", "region"],
    "sample_size": 1000,  // Can be integer or fraction (0.1 = 10%)
    "min_per_stratum": 10,  // Optional
    "seed": 42  // Optional
  }
}
```

#### Systematic Sampling
```json
{
  "method": "systematic",
  "parameters": {
    "interval": 10,  // Take every 10th record
    "start": 0  // Optional, default is 0
  }
}
```

#### Cluster Sampling
```json
{
  "method": "cluster",
  "parameters": {
    "cluster_column": "store_id",
    "num_clusters": 20,
    "sample_within_clusters": false  // Optional
  }
}
```

#### Weighted Sampling
```json
{
  "method": "weighted",
  "parameters": {
    "weight_column": "importance_score",
    "sample_size": 1000,
    "seed": 42  // Optional
  }
}
```

#### Custom Sampling
```json
{
  "method": "custom",
  "parameters": {
    "query": "category = 'Electronics' AND price > 500"
  }
}
```

---

## 3. Get Multi-Round Job Status

Check the status and results of a multi-round sampling job.

### Endpoint
`GET /api/sampling/multi-round/jobs/{job_id}`

### Request
```bash
curl -X GET "http://localhost:8000/api/sampling/multi-round/jobs/28" \
  -H "Authorization: Bearer <token>"
```

### Response (Completed)
```json
{
  "run_id": "28",
  "status": "completed",
  "message": "Multi-round sampling completed. 2 rounds processed.",
  "total_rounds": 2,
  "completed_rounds": 2,
  "current_round": null,
  "round_results": [
    {
      "round_number": 1,
      "method": "random",
      "sample_size": 1000,
      "output_uri": "file:///data/samples/14/14/multi_round/28/1.parquet",
      "preview": [
        {"product": "Laptop", "quantity": 10, "price": 999.99},
        {"product": "Mouse", "quantity": 50, "price": 25.99}
      ],
      "summary": {
        "total_rows": 1000,
        "total_columns": 3,
        "column_types": {
          "product": "VARCHAR",
          "quantity": "BIGINT",
          "price": "DOUBLE"
        },
        "memory_usage_mb": 0.02,
        "null_counts": {"product": 0, "quantity": 0, "price": 0}
      },
      "started_at": "2025-06-22T11:08:36.301511",
      "completed_at": "2025-06-22T11:08:36.319314"
    },
    {
      "round_number": 2,
      "method": "stratified",
      "sample_size": 500,
      "output_uri": "file:///data/samples/14/14/multi_round/28/2.parquet",
      "preview": [...],
      "summary": {...},
      "started_at": "2025-06-22T11:08:36.320000",
      "completed_at": "2025-06-22T11:08:36.340000"
    }
  ],
  "residual_uri": "file:///data/samples/14/14/multi_round/28/residual.parquet",
  "residual_size": 2500,
  "residual_summary": {
    "total_rows": 2500,
    "total_columns": 3,
    "column_types": {...},
    "memory_usage_mb": 0.05,
    "null_counts": {...}
  },
  "error_message": null,
  "created_at": "2025-06-22T16:08:36.269584",
  "started_at": "2025-06-22T16:08:36.300000",
  "completed_at": "2025-06-22T16:08:36.350000"
}
```

### Response (Failed)
```json
{
  "run_id": "27",
  "status": "failed",
  "message": "Multi-round sampling failed: An unexpected error occurred",
  "total_rounds": 1,
  "completed_rounds": 0,
  "current_round": null,
  "round_results": [],
  "residual_uri": null,
  "residual_size": null,
  "residual_summary": null,
  "error_message": "An unexpected error occurred: the JSON object must be str, bytes or bytearray, not dict",
  "created_at": "2025-06-22T16:02:25.324852",
  "started_at": "2025-06-22T16:02:25.325000",
  "completed_at": "2025-06-22T16:02:25.328000"
}
```

---

## 4. Get Merged Sample Data

Retrieve the final merged sample file that combines all sampling rounds.

### Endpoint
`GET /api/sampling/multi-round/jobs/{job_id}/merged-sample`

### Query Parameters
- `page` (int, default=1): Page number (1-indexed)
- `page_size` (int, default=100): Number of items per page
- `columns` (array, optional): Specific columns to return
- `export_format` (string, optional): Export format (csv, json)

### Request
```bash
curl -X GET "http://localhost:8000/api/sampling/multi-round/jobs/28/merged-sample?page=1&page_size=10&columns=product,price" \
  -H "Authorization: Bearer <token>"
```

### Response
```json
{
  "data": [
    {"product": "Laptop", "price": 999.99},
    {"product": "Mouse", "price": 25.99},
    {"product": "Keyboard", "price": 45.99},
    {"product": "Monitor", "price": 299.99},
    {"product": "Headphones", "price": 89.99},
    {"product": "Webcam", "price": 79.99},
    {"product": "Speaker", "price": 149.99},
    {"product": "USB Hub", "price": 29.99},
    {"product": "HDMI Cable", "price": 15.99},
    {"product": "Power Strip", "price": 35.99}
  ],
  "pagination": {
    "page": 1,
    "page_size": 10,
    "total_items": 1500,
    "total_pages": 150,
    "has_next": true,
    "has_previous": false
  },
  "columns": ["product", "price"],
  "summary": {
    "total_rounds": 2,
    "completed_rounds": 2,
    "round_results": [...],
    "total_samples": 1500,
    "residual_info": {...}
  },
  "file_path": "/data/samples/14/14/multi_round/28/0.parquet",
  "job_id": "28"
}
```

### Export as CSV
```bash
curl -X GET "http://localhost:8000/api/sampling/multi-round/jobs/28/merged-sample?export_format=csv&page=1&page_size=100" \
  -H "Authorization: Bearer <token>"
```

Response:
```json
{
  "format": "csv",
  "data": "product,quantity,price\nLaptop,10,999.99\nMouse,50,25.99\n...",
  "filename": "job_28_page_1.csv"
}
```

---

## 5. Get Round Preview

Get preview data from a specific sampling round.

### Endpoint
`GET /api/sampling/multi-round/jobs/{job_id}/round/{round_number}/preview`

### Request
```bash
curl -X GET "http://localhost:8000/api/sampling/multi-round/jobs/28/round/1/preview?page=1&page_size=5" \
  -H "Authorization: Bearer <token>"
```

### Response
```json
{
  "data": [
    {"product": "Laptop", "quantity": 10, "price": 999.99},
    {"product": "Mouse", "quantity": 50, "price": 25.99},
    {"product": "Keyboard", "quantity": 30, "price": 45.99},
    {"product": "Monitor", "quantity": 15, "price": 299.99},
    {"product": "Headphones", "quantity": 25, "price": 89.99}
  ],
  "round_info": {
    "round_number": 1,
    "method": "random",
    "sample_size": 1000,
    "output_uri": "file:///data/samples/14/14/multi_round/28/1.parquet"
  },
  "pagination": {
    "page": 1,
    "page_size": 5,
    "total_items": 10,
    "total_pages": 2,
    "has_next": true,
    "has_previous": false
  }
}
```

---

## 6. Get Residual Preview

Get preview data from the final residual dataset.

### Endpoint
`GET /api/sampling/multi-round/jobs/{job_id}/residual/preview`

### Request
```bash
curl -X GET "http://localhost:8000/api/sampling/multi-round/jobs/28/residual/preview?page=1&page_size=5" \
  -H "Authorization: Bearer <token>"
```

### Response
```json
{
  "data": [],
  "residual_info": {
    "size": 2500,
    "uri": "file:///data/samples/14/14/multi_round/28/residual.parquet",
    "summary": {
      "total_rows": 2500,
      "total_columns": 3,
      "column_types": {
        "product": "VARCHAR",
        "quantity": "BIGINT",
        "price": "DOUBLE"
      },
      "memory_usage_mb": 0.05,
      "null_counts": {
        "product": 0,
        "quantity": 0,
        "price": 0
      }
    }
  },
  "pagination": {
    "page": 1,
    "page_size": 5,
    "total_items": 0,
    "total_pages": 0,
    "has_next": false,
    "has_previous": false
  }
}
```

---

## 7. Execute Multi-Round Sampling Synchronously

Execute multi-round sampling synchronously and return all results directly (for smaller datasets).

### Endpoint
`POST /api/sampling/{dataset_id}/{version_id}/multi-round/execute`

### Request
```bash
curl -X POST "http://localhost:8000/api/sampling/14/14/multi-round/execute?page=1&page_size=100" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "rounds": [
      {
        "round_number": 1,
        "method": "random",
        "parameters": {"sample_size": 10, "seed": 42},
        "output_name": "round_1_random"
      }
    ],
    "export_residual": true,
    "residual_output_name": "final_residual"
  }'
```

### Response
```json
{
  "rounds": [
    {
      "round_number": 1,
      "method": "random",
      "sample_size": 10,
      "data": [
        {"product": "Laptop", "quantity": 10, "price": 999.99},
        {"product": "Mouse", "quantity": 50, "price": 25.99},
        ...
      ],
      "summary": {
        "total_rows": 10,
        "total_columns": 3,
        "column_types": {...},
        "memory_usage_mb": 0.001,
        "null_counts": {...}
      },
      "pagination": {
        "page": 1,
        "page_size": 100,
        "total_items": 10,
        "total_pages": 1,
        "has_next": false,
        "has_previous": false
      }
    }
  ],
  "residual": {
    "size": 390,
    "data": [
      {"product": "Webcam", "quantity": 20, "price": 79.99},
      ...
    ],
    "summary": {
      "total_rows": 390,
      "total_columns": 3,
      "column_types": {...},
      "memory_usage_mb": 0.008,
      "null_counts": {...}
    },
    "pagination": {
      "page": 1,
      "page_size": 100,
      "total_items": 390,
      "total_pages": 4,
      "has_next": true,
      "has_previous": false
    }
  }
}
```

---

## 8. Get Samplings by User ID

Retrieve all sampling runs created by a specific user.

### Endpoint
`GET /api/sampling/user/{user_id}/samplings`

### Query Parameters
- `page` (int, default=1): Page number (1-indexed)
- `page_size` (int, default=10): Number of items per page

### Request
```bash
curl -X GET "http://localhost:8000/api/sampling/user/1/samplings?page=1&page_size=10" \
  -H "Authorization: Bearer <token>"
```

### Response
```json
{
  "runs": [
    {
      "id": 28,
      "dataset_id": 14,
      "dataset_version_id": 14,
      "dataset_name": "Product Inventory",
      "version_number": 1,
      "user_id": 1,
      "user_soeid": "bg54677",
      "run_type": "sampling",
      "status": "completed",
      "run_timestamp": "2025-06-22T16:08:36.269584",
      "execution_time_ms": 54,
      "notes": null,
      "output_file_id": 45,
      "output_file_path": "/home/saketh/Projects/dsa/src/data/samples/14/14/multi_round/28/round_0.parquet",
      "output_file_size": 728,
      "run_parameters": {
        "request": {
          "rounds": [
            {
              "round_number": 1,
              "method": "random",
              "parameters": {"sample_size": 2, "seed": 42},
              "output_name": "round_1_random"
            }
          ],
          "export_residual": true,
          "residual_output_name": "final_residual"
        },
        "job_type": "multi_round_sampling",
        "total_rounds": 1,
        "completed_rounds": 1
      },
      "output_summary": {
        "total_rounds": 1,
        "completed_rounds": 1,
        "round_results": [...],
        "total_samples": 2,
        "residual_info": {...}
      }
    },
    {
      "id": 27,
      "dataset_id": 16,
      "dataset_version_id": 18,
      "dataset_name": "Electric_Vehicle_Population_Data",
      "version_number": 1,
      "user_id": 1,
      "user_soeid": "bg54677",
      "run_type": "sampling",
      "status": "failed",
      "run_timestamp": "2025-06-22T16:02:25.324852",
      "execution_time_ms": 4,
      "notes": "An unexpected error occurred: the JSON object must be str, bytes or bytearray, not dict",
      "output_file_id": null,
      "output_file_path": null,
      "output_file_size": null,
      "run_parameters": {...},
      "output_summary": null
    }
  ],
  "total_count": 3,
  "page": 1,
  "page_size": 10,
  "status": "pending"
}
```

---

## 9. Get Samplings by Dataset Version ID

Retrieve all sampling runs performed on a specific dataset version.

### Endpoint
`GET /api/sampling/dataset-version/{dataset_version_id}/samplings`

### Query Parameters
- `page` (int, default=1): Page number (1-indexed)
- `page_size` (int, default=10): Number of items per page

### Request
```bash
curl -X GET "http://localhost:8000/api/sampling/dataset-version/13/samplings?page=1&page_size=10" \
  -H "Authorization: Bearer <token>"
```

### Response
```json
{
  "runs": [
    {
      "id": 26,
      "dataset_id": 12,
      "dataset_version_id": 13,
      "dataset_name": "database_pokemon",
      "version_number": 2,
      "user_id": 1,
      "user_soeid": "bg54677",
      "run_type": "sampling",
      "status": "failed",
      "run_timestamp": "2025-06-22T15:29:33.320200",
      "execution_time_ms": 4,
      "notes": "An unexpected error occurred: the JSON object must be str, bytes or bytearray, not dict",
      "output_file_id": null,
      "output_file_path": null,
      "output_file_size": null,
      "run_parameters": {
        "request": {
          "rounds": [
            {
              "round_number": 1,
              "method": "random",
              "parameters": {"sample_size": 1000},
              "output_name": "round_1_sample"
            }
          ],
          "export_residual": false
        },
        "job_type": "multi_round_sampling",
        "total_rounds": 1,
        "completed_rounds": 0
      },
      "output_summary": null
    }
  ],
  "total_count": 1,
  "page": 1,
  "page_size": 10,
  "status": "pending"
}
```

---

## 10. Get Samplings by Dataset ID

Retrieve all sampling runs performed on a specific dataset across all its versions.

### Endpoint
`GET /api/sampling/dataset/{dataset_id}/samplings`

### Query Parameters
- `page` (int, default=1): Page number (1-indexed)
- `page_size` (int, default=10): Number of items per page

### Request
```bash
curl -X GET "http://localhost:8000/api/sampling/dataset/12/samplings?page=1&page_size=10" \
  -H "Authorization: Bearer <token>"
```

### Response
```json
{
  "runs": [
    {
      "id": 26,
      "dataset_id": 12,
      "dataset_version_id": 13,
      "dataset_name": "database_pokemon",
      "version_number": 2,
      "user_id": 1,
      "user_soeid": "bg54677",
      "run_type": "sampling",
      "status": "failed",
      "run_timestamp": "2025-06-22T15:29:33.320200",
      "execution_time_ms": 4,
      "notes": "An unexpected error occurred: the JSON object must be str, bytes or bytearray, not dict",
      "output_file_id": null,
      "output_file_path": null,
      "output_file_size": null,
      "run_parameters": {...},
      "output_summary": null
    },
    {
      "id": 24,
      "dataset_id": 12,
      "dataset_version_id": 12,
      "dataset_name": "database_pokemon",
      "version_number": 1,
      "user_id": 1,
      "user_soeid": "bg54677",
      "run_type": "sampling",
      "status": "completed",
      "run_timestamp": "2025-06-21T14:20:00.000000",
      "execution_time_ms": 120,
      "notes": null,
      "output_file_id": 42,
      "output_file_path": "/data/samples/12/12/multi_round/24/0.parquet",
      "output_file_size": 2048,
      "run_parameters": {...},
      "output_summary": {...}
    }
  ],
  "total_count": 2,
  "page": 1,
  "page_size": 10,
  "status": "pending"
}
```

---

## Error Responses

All endpoints follow a consistent error response format:

### 400 Bad Request
```json
{
  "detail": "Validation error: Dataset version with ID 999 not found"
}
```

### 401 Unauthorized
```json
{
  "detail": "Could not validate credentials"
}
```

### 404 Not Found
```json
{
  "detail": "Multi-round sampling job with ID abc123 not found"
}
```

### 500 Internal Server Error
```json
{
  "detail": "Error executing sampling: Database connection failed"
}
```

---

## Common Use Cases

### 1. Simple Random Sampling
```json
{
  "rounds": [
    {
      "round_number": 1,
      "method": "random",
      "parameters": {"sample_size": 1000, "seed": 42},
      "output_name": "random_sample"
    }
  ],
  "export_residual": false
}
```

### 2. Progressive Sampling with Different Methods
```json
{
  "rounds": [
    {
      "round_number": 1,
      "method": "stratified",
      "parameters": {
        "strata_columns": ["region"],
        "sample_size": 0.2,
        "min_per_stratum": 100
      },
      "output_name": "regional_sample"
    },
    {
      "round_number": 2,
      "method": "random",
      "parameters": {"sample_size": 500},
      "output_name": "additional_random",
      "filters": {
        "conditions": [
          {"column": "created_date", "operator": ">", "value": "2024-01-01"}
        ],
        "logic": "AND"
      }
    }
  ],
  "export_residual": true,
  "residual_output_name": "remaining_data"
}
```

### 3. Filtered Sampling with Column Selection
```json
{
  "rounds": [
    {
      "round_number": 1,
      "method": "systematic",
      "parameters": {"interval": 5},
      "output_name": "systematic_sample",
      "filters": {
        "conditions": [
          {"column": "status", "operator": "=", "value": "active"},
          {"column": "score", "operator": ">=", "value": 80}
        ],
        "logic": "AND"
      },
      "selection": {
        "columns": ["id", "name", "score", "status"]
      }
    }
  ],
  "export_residual": false
}
```

### 4. Complex Nested Filtering
```json
{
  "rounds": [
    {
      "round_number": 1,
      "method": "random",
      "parameters": {"sample_size": 1000},
      "output_name": "complex_filter_sample",
      "filters": {
        "groups": [
          {
            "conditions": [
              {"column": "category", "operator": "=", "value": "A"},
              {"column": "price", "operator": ">", "value": 100}
            ],
            "logic": "AND"
          },
          {
            "conditions": [
              {"column": "category", "operator": "=", "value": "B"},
              {"column": "price", "operator": ">", "value": 200}
            ],
            "logic": "AND"
          }
        ],
        "logic": "OR"
      }
    }
  ],
  "export_residual": false
}
```

---

## Notes

1. **Pagination**: All list endpoints support pagination with `page` and `page_size` parameters.
2. **File Storage**: Output files are stored in parquet format at `/data/samples/{dataset_id}/{version_id}/multi_round/{job_id}/`.
3. **Async vs Sync**: Use async endpoints for large datasets, sync endpoints for smaller datasets or when you need immediate results.
4. **Residual Export**: When `export_residual` is true, the remaining unsampled data is saved for further analysis.
5. **Progressive Sampling**: Each round samples from the residual of previous rounds, ensuring no overlap between rounds.