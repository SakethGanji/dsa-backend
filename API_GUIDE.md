# DSA API Guide

## Overview
This guide provides a comprehensive reference for all API endpoints in the DSA (Data Science API) platform, organized by vertical slice.

## Table of Contents
1. [Users API](#1-users-api) - Authentication and user management
2. [Datasets API](#2-datasets-api) - Dataset management, versioning, and permissions
3. [Search API](#3-search-api) - Advanced search and suggestions
4. [Explore API](#4-explore-api) - Data profiling and exploration
5. [Sampling API](#5-sampling-api) - Multi-round sampling and analysis

## Base URL
```
http://localhost:8000
```

## Health Check

#### API Status
**GET** `/`

Check if the API is running.

**Response:**
```json
{
  "message": "Data Science API is running"
}
```

#### Health Check
**GET** `/health`

Get health status of the API.

**Response:**
```json
{
  "status": "healthy"
}
```

## Authentication
Most endpoints require authentication using Bearer tokens (JWT).

### Headers
```
Authorization: Bearer <access_token>
Content-Type: application/json
```

---

## 1. Users API

### Authentication

#### Login (OAuth2 Token)
**POST** `/api/users/token`

OAuth2 compatible token login endpoint. Returns both access and refresh tokens.

**Request:** (application/x-www-form-urlencoded)
```
username=user123&password=password123
```

**Note:** This endpoint uses OAuth2PasswordRequestForm which requires form-data format, not JSON.

**Response:**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIs...",
  "refresh_token": "eyJhbGciOiJIUzI1NiIs...",
  "token_type": "bearer"
}
```

#### Refresh Token
**POST** `/api/users/token/refresh`

Get new access token using refresh token.

**Request Body:**
```
refresh_token: eyJhbGciOiJIUzI1NiIs...
```

**Response:**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIs...",
  "refresh_token": "eyJhbGciOiJIUzI1NiIs...",
  "token_type": "bearer"
}
```

### User Management

#### Register User
**POST** `/api/users/register`

Register a new user (public endpoint).

**Request:**
```json
{
  "soeid": "ABC1234",
  "password": "securepassword123",
  "role_id": 1
}
```

**Response:**
```json
{
  "id": 1,
  "soeid": "ABC1234",
  "role_id": 1,
  "role_name": "user",
  "created_at": "2024-01-15T10:30:00Z",
  "updated_at": "2024-01-15T10:30:00Z"
}
```

#### List Users
**GET** `/api/users/`

List all users (requires authentication).

**Response:**
```json
[
  {
    "id": 1,
    "soeid": "ABC1234",
    "role_id": 1,
    "role_name": "user",
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z"
  }
]
```

#### Create User (Admin)
**POST** `/api/users/`

Create a new user (requires authentication, for admin use).

**Request:**
```json
{
  "soeid": "XYZ5678",
  "password": "securepassword123",
  "role_id": 2
}
```

**Response:**
```json
{
  "id": 2,
  "soeid": "XYZ5678",
  "role_id": 2,
  "role_name": "user",
  "created_at": "2024-01-15T10:30:00Z",
  "updated_at": "2024-01-15T10:30:00Z"
}
```

---

## 2. Datasets API

### Dataset Management

#### Upload Dataset
**POST** `/api/datasets/upload`

Upload a new dataset or create a new version of an existing dataset.

**Request:** (multipart/form-data)
```
file: <file.csv/xlsx/xls>
name: "Sales Data 2024"
description: "Monthly sales report" (optional)
tags: ["sales", "2024", "monthly"] (optional, as JSON array or comma-separated)
dataset_id: 1 (optional, for creating new version)
```

**Response:**
```json
{
  "dataset": {
    "id": 1,
    "name": "Sales Data 2024",
    "description": "Monthly sales report",
    "tags": ["sales", "2024", "monthly"],
    "created_at": "2024-01-15T10:30:00Z",
    "created_by": 1,
    "current_version": 1,
    "file_size": 1024000
  },
  "version": {
    "id": 1,
    "dataset_id": 1,
    "version_number": 1,
    "file_name": "sales_data.csv",
    "file_size": 1024000,
    "created_at": "2024-01-15T10:30:00Z",
    "created_by": 1
  },
  "sheets": [
    {
      "name": "Sheet1",
      "row_count": 1000,
      "column_count": 10
    }
  ]
}
```

#### List All Tags
**GET** `/api/datasets/tags`

Get all unique tags used across datasets with usage counts.

**Response:**
```json
[
  {
    "name": "sales",
    "count": 15
  },
  {
    "name": "2024",
    "count": 10
  },
  {
    "name": "monthly",
    "count": 8
  }
]
```

#### List Datasets
**GET** `/api/datasets`

List datasets with filtering, sorting, and pagination.

**Query Parameters:**
- `limit`: Page size (default: 20, max: 100)
- `offset`: Number of items to skip (default: 0)
- `name`: Filter by name (partial match)
- `description`: Filter by description (partial match)
- `created_by`: Filter by creator user ID
- `tags`: Filter by tags (array)
- `sort_by`: Sort field (name, created_at, updated_at, file_size, current_version)
- `sort_order`: asc or desc

**Response:**
```json
[
  {
    "id": 1,
    "name": "Sales Data 2024",
    "description": "Monthly sales report",
    "tags": ["sales", "2024"],
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z",
    "created_by": 1,
    "current_version": 3,
    "file_size": 1124000,
    "permissions": {
      "can_read": true,
      "can_write": true,
      "can_delete": false,
      "is_admin": false
    }
  }
]
```

#### Get Dataset Details
**GET** `/api/datasets/{dataset_id}`

Get detailed information about a specific dataset.

**Response:**
```json
{
  "id": 1,
  "name": "Sales Data 2024",
  "description": "Monthly sales report",
  "tags": ["sales", "2024", "monthly"],
  "created_at": "2024-01-15T10:30:00Z",
  "updated_at": "2024-01-20T10:30:00Z",
  "created_by": 1,
  "current_version": 3,
  "file_size": 1124000,
  "versions": [
    {
      "id": 3,
      "version_number": 3,
      "file_name": "sales_data_v3.csv",
      "file_size": 1124000,
      "created_at": "2024-01-20T10:30:00Z"
    }
  ],
  "permissions": {
    "can_read": true,
    "can_write": true,
    "can_delete": false,
    "is_admin": false
  }
}
```

#### Update Dataset
**PATCH** `/api/datasets/{dataset_id}`

Update dataset metadata.

**Request:**
```json
{
  "name": "Sales Data 2024 - Updated",
  "description": "Updated monthly sales report",
  "tags": ["sales", "2024", "monthly", "q1"]
}
```

**Response:** Same as Get Dataset Details

#### Delete Dataset
**DELETE** `/api/datasets/{dataset_id}`

Delete a dataset and all its versions (requires admin permission).

**Response:** 204 No Content

### Version Management

#### List Versions
**GET** `/api/datasets/{dataset_id}/versions`

List all versions of a dataset.

**Response:**
```json
[
  {
    "id": 3,
    "dataset_id": 1,
    "version_number": 3,
    "file_name": "sales_data_v3.csv",
    "file_size": 1124000,
    "created_at": "2024-01-20T10:30:00Z",
    "created_by": "ABC1234",
    "tags": ["latest", "production"]
  },
  {
    "id": 2,
    "dataset_id": 1,
    "version_number": 2,
    "file_name": "sales_data_v2.csv",
    "file_size": 1024000,
    "created_at": "2024-01-18T10:30:00Z",
    "created_by": "ABC1234",
    "tags": ["archived"]
  }
]
```

#### Get Latest Version
**GET** `/api/datasets/{dataset_id}/versions/latest`

Get the most recent version.

**Response:** Same as version details

#### Get Version by Number
**GET** `/api/datasets/{dataset_id}/versions/number/{version_number}`

Get a specific version by its version number.

**Response:** Same as version details

#### Get Version by Tag
**GET** `/api/datasets/{dataset_id}/versions/tag/{tag_name}`

Get the version associated with a specific tag.

**Response:** Same as version details

#### Get Version Details
**GET** `/api/datasets/{dataset_id}/versions/{version_id}`

Get detailed information about a specific version.

**Response:**
```json
{
  "id": 3,
  "dataset_id": 1,
  "version_number": 3,
  "file_name": "sales_data_v3.csv",
  "file_size": 1124000,
  "created_at": "2024-01-20T10:30:00Z",
  "created_by": "ABC1234",
  "tags": ["latest", "production"],
  "sheets": ["Sheet1", "Sheet2"]
}
```

#### Delete Version
**DELETE** `/api/datasets/{dataset_id}/versions/{version_id}`

Delete a specific version (requires write permission).

**Response:** 204 No Content

#### Create Version from Changes
**POST** `/api/datasets/{dataset_id}/versions`

Create a new dataset version using overlay-based file changes.

**Request:**
```json
{
  "dataset_id": 1,
  "base_version_id": 3,
  "description": "Updated Q1 data",
  "changes": [
    {
      "operation": "add",
      "file_path": "new_data.csv"
    }
  ]
}
```

**Response:**
```json
{
  "version": {
    "id": 4,
    "dataset_id": 1,
    "version_number": 4,
    "created_at": "2024-01-25T10:30:00Z"
  },
  "message": "Version created successfully"
}
```

#### Download Version
**GET** `/api/datasets/{dataset_id}/versions/{version_id}/download`

Download the dataset file.

**Response:** Binary file stream

### Version Schema

#### Get Version Schema
**GET** `/api/datasets/{dataset_id}/versions/{version_id}/schema`

Get the schema of a dataset version.

**Response:**
```json
{
  "version_id": 3,
  "schema": {
    "columns": [
      {
        "name": "id",
        "type": "integer",
        "nullable": false
      },
      {
        "name": "date",
        "type": "date",
        "nullable": false
      },
      {
        "name": "product",
        "type": "string",
        "nullable": false,
        "max_length": 100
      },
      {
        "name": "price",
        "type": "decimal",
        "nullable": false,
        "precision": 10,
        "scale": 2
      }
    ]
  }
}
```

#### Compare Version Schemas
**POST** `/api/datasets/{dataset_id}/schema/compare`

Compare schemas between two versions.

**Request:**
```json
{
  "version_id_1": 2,
  "version_id_2": 3
}
```

**Response:**
```json
{
  "identical": false,
  "changes": [
    {
      "column": "discount",
      "change_type": "added",
      "version_2_details": {
        "type": "decimal",
        "nullable": true
      }
    },
    {
      "column": "category",
      "change_type": "type_changed",
      "version_1_type": "string",
      "version_2_type": "enum"
    }
  ]
}
```

### File Attachments

#### Attach Files to Version
**POST** `/api/datasets/{dataset_id}/versions/{version_id}/files`

Attach additional files to a dataset version.

**Request:** (multipart/form-data)
```
file: <documentation.pdf>
component_type: "documentation"
description: "User guide for dataset"
```

**Response:**
```json
{
  "file_id": 1,
  "version_id": 3,
  "component_type": "documentation",
  "file_name": "documentation.pdf",
  "file_size": 524288,
  "uploaded_at": "2024-01-25T10:30:00Z"
}
```

#### List Version Files
**GET** `/api/datasets/{dataset_id}/versions/{version_id}/files`

List all files attached to a version.

**Response:**
```json
[
  {
    "file_id": 1,
    "component_type": "documentation",
    "file_name": "documentation.pdf",
    "file_size": 524288,
    "uploaded_at": "2024-01-25T10:30:00Z",
    "uploaded_by": "ABC1234"
  },
  {
    "file_id": 2,
    "component_type": "schema",
    "file_name": "schema.json",
    "file_size": 2048,
    "uploaded_at": "2024-01-25T10:35:00Z",
    "uploaded_by": "ABC1234"
  }
]
```

#### Get Specific File
**GET** `/api/datasets/{dataset_id}/versions/{version_id}/files/{component_type}`

Download a specific file by component type.

**Response:** Binary file stream

### Data Access

#### List Sheets
**GET** `/api/datasets/{dataset_id}/versions/{version_id}/sheets`

List all sheets in a version.

**Response:**
```json
[
  {
    "name": "Sheet1",
    "row_count": 1000,
    "column_count": 10,
    "columns": ["id", "date", "product", "quantity", "price"]
  }
]
```

#### Get Data
**GET** `/api/datasets/{dataset_id}/versions/{version_id}/data`

Get paginated data from a sheet.

**Query Parameters:**
- `sheet`: Sheet name (default: first sheet)
- `page`: Page number (default: 1)
- `page_size`: Rows per page (default: 100, max: 1000)

**Response:**
```json
{
  "data": [
    {
      "id": 1,
      "date": "2024-01-01",
      "product": "Widget A",
      "quantity": 100,
      "price": 25.99
    }
  ],
  "total_rows": 1000,
  "page": 1,
  "page_size": 100,
  "total_pages": 10
}
```

### Permissions

#### List Permissions
**GET** `/api/datasets/{dataset_id}/permissions`

List all permissions for a dataset.

**Response:**
```json
[
  {
    "user_id": 1,
    "user_soeid": "ABC1234",
    "permission_type": "admin",
    "granted_at": "2024-01-15T10:30:00Z",
    "granted_by": "XYZ9876"
  }
]
```

#### Grant Permission
**POST** `/api/datasets/{dataset_id}/permissions`

Grant permission to a user.

**Request:**
```json
{
  "user_id": 2,
  "permission_type": "read"
}
```

**Response:**
```json
{
  "user_id": 2,
  "dataset_id": 1,
  "permission_type": "read",
  "granted_at": "2024-01-25T10:30:00Z"
}
```

#### Revoke Permission
**DELETE** `/api/datasets/{dataset_id}/permissions/{user_id}/{permission_type}`

Revoke a specific permission from a user.

**Response:**
```json
{
  "message": "Permission revoked successfully",
  "user_id": 2,
  "permission_type": "read"
}
```

### Version Tags

#### Create Version Tag
**POST** `/api/datasets/{dataset_id}/tags/{tag_name}`

Create or update a tag pointing to a specific version.

**Request:**
```json
{
  "version_id": 3,
  "description": "Production release"
}
```

**Response:**
```json
{
  "tag_name": "production",
  "version_id": 3,
  "created_at": "2024-01-25T10:30:00Z",
  "created_by": "ABC1234"
}
```

#### List Version Tags
**GET** `/api/datasets/{dataset_id}/tags`

List all tags for a dataset.

**Response:**
```json
[
  {
    "tag_name": "production",
    "version_id": 3,
    "version_number": 3,
    "created_at": "2024-01-25T10:30:00Z",
    "created_by": "ABC1234"
  },
  {
    "tag_name": "staging",
    "version_id": 4,
    "version_number": 4,
    "created_at": "2024-01-26T10:30:00Z",
    "created_by": "XYZ9876"
  }
]
```

#### Get Version Tag
**GET** `/api/datasets/{dataset_id}/tags/{tag_name}`

Get details about a specific tag.

**Response:**
```json
{
  "tag_name": "production",
  "version_id": 3,
  "version_number": 3,
  "description": "Production release",
  "created_at": "2024-01-25T10:30:00Z",
  "created_by": "ABC1234"
}
```

#### Delete Version Tag
**DELETE** `/api/datasets/{dataset_id}/tags/{tag_name}`

Delete a version tag.

**Response:**
```json
{
  "message": "Tag deleted successfully",
  "tag_name": "staging"
}
```

### Statistics

#### Get Latest Version Statistics
**GET** `/api/datasets/{dataset_id}/statistics`

Get statistics for the latest version.

**Response:**
```json
{
  "dataset_id": 1,
  "version_id": 3,
  "total_rows": 1000,
  "total_columns": 10,
  "file_size": 1124000,
  "columns": [
    {
      "column_name": "price",
      "data_type": "numeric",
      "null_count": 0,
      "unique_count": 250,
      "min": 0.99,
      "max": 999.99,
      "mean": 125.50,
      "median": 99.99,
      "std_dev": 85.25
    }
  ],
  "computed_at": "2024-01-20T10:35:00Z"
}
```

#### Get Version Statistics
**GET** `/api/datasets/{dataset_id}/versions/{version_id}/statistics`

Get pre-computed statistics for a specific dataset version.

**Response:** Same as above

#### Refresh Statistics
**POST** `/api/datasets/{dataset_id}/versions/{version_id}/statistics/refresh`

Recalculate statistics for a version.

**Query Parameters:**
- `detailed`: Calculate detailed statistics including histograms (default: false)
- `sample_size`: Number of rows to sample for detailed statistics (optional)

**Response:**
```json
{
  "status": "success",
  "statistics": {
    "dataset_id": 1,
    "version_id": 3,
    "total_rows": 1000,
    "total_columns": 10,
    "file_size": 1124000,
    "computed_at": "2024-01-20T10:35:00Z"
  },
  "computation_time_seconds": 2.5
}
```

---

## 3. Search API

#### Search Datasets
**GET** `/api/datasets/search`

Advanced search with full-text search, filtering, and faceting.

**Query Parameters:**
- `query`: Search query string
- `tags`: Filter by tags (array, AND logic)
- `file_types`: Filter by file types (array)
- `created_by`: Filter by creator user IDs (array)
- `created_after`: Filter datasets created after this date (ISO format: 2024-01-01T00:00:00Z)
- `created_before`: Filter datasets created before this date
- `updated_after`: Filter datasets updated after this date
- `updated_before`: Filter datasets updated before this date
- `size_min`: Minimum file size in bytes
- `size_max`: Maximum file size in bytes
- `versions_min`: Minimum number of versions
- `versions_max`: Maximum number of versions
- `fuzzy`: Enable fuzzy/typo-tolerant search (default: false)
- `search_description`: Include description in search (default: true)
- `search_tags`: Include tags in search (default: true)
- `limit`: Results per page (default: 20, max: 100)
- `offset`: Number of results to skip
- `sort_by`: Sort field (relevance, name, created_at, updated_at, file_size, version_count)
- `sort_order`: asc or desc (default: desc)
- `include_facets`: Include facet counts (default: true)
- `facet_fields`: Specific facet fields to include

**Response:**
```json
{
  "results": [
    {
      "dataset": {
        "id": 1,
        "name": "Sales Data 2024",
        "description": "Monthly sales report",
        "tags": ["sales", "2024"],
        "created_at": "2024-01-15T10:30:00Z",
        "updated_at": "2024-01-20T10:30:00Z",
        "created_by": 1,
        "current_version": 3,
        "file_size": 1124000,
        "version_count": 3
      },
      "score": 0.95,
      "highlights": {
        "name": ["<em>Sales</em> Data 2024"],
        "description": ["Monthly <em>sales</em> report"]
      }
    }
  ],
  "total": 15,
  "pagination": {
    "limit": 20,
    "offset": 0,
    "total_pages": 1
  },
  "facets": {
    "tags": [
      {"value": "sales", "count": 10},
      {"value": "2024", "count": 8}
    ],
    "file_types": [
      {"value": "csv", "count": 12},
      {"value": "xlsx", "count": 3}
    ],
    "created_by": [
      {"value": 1, "count": 8},
      {"value": 2, "count": 7}
    ]
  },
  "query_info": {
    "original_query": "sales",
    "processed_query": "sales",
    "fuzzy_enabled": false
  }
}
```

#### Search Suggestions
**GET** `/api/datasets/search/suggest`

Get autocomplete suggestions for search queries.

**Query Parameters:**
- `query`: Partial search query (required, min length: 1)
- `limit`: Maximum suggestions (default: 10, max: 50)
- `types`: Types to include (array, options: dataset_names, tags)

**Response:**
```json
{
  "suggestions": [
    {
      "text": "sales data",
      "type": "dataset_name",
      "dataset_id": 1,
      "score": 0.98
    },
    {
      "text": "sales",
      "type": "tag",
      "usage_count": 15,
      "score": 0.95
    }
  ],
  "query": "sal",
  "total_suggestions": 2
}
```

---

## 4. Explore API

#### Explore Dataset
**POST** `/api/explore/{dataset_id}/{version_id}`

Load a dataset and generate a profile report or summary.

**Request:**
```json
{
  "sheet": "Sheet1",
  "format": "json",
  "run_profiling": true,
  "sample_size": 10000,
  "sampling_method": "random",
  "auto_sample_threshold": 100000
}
```

**Request Parameters:**
- `sheet`: Sheet name (optional for single-sheet files)
- `format`: Output format ("json" or "html", default: "json")
- `run_profiling`: Whether to run full profiling (default: true)
- `sample_size`: Number of rows to sample (optional)
- `sampling_method`: Method for sampling ("random" or "head", default: "random")
- `auto_sample_threshold`: Threshold for automatic sampling (default: 100000)

**Response (JSON format):**
```json
{
  "format": "json",
  "summary": {
    "dataset_name": "Sales Data 2024",
    "version": 3,
    "sheet": "Sheet1",
    "total_rows": 100000,
    "sampled_rows": 10000,
    "columns": 10
  },
  "profile": {
    "variables": {
      "price": {
        "type": "numeric",
        "missing": 0,
        "distinct": 2500,
        "min": 0.99,
        "max": 999.99,
        "mean": 125.50,
        "std": 85.25,
        "percentiles": {
          "25%": 45.99,
          "50%": 99.99,
          "75%": 189.99
        }
      }
    },
    "correlations": {
      "pearson": {
        "price_quantity": -0.15
      }
    },
    "missing": {
      "count": 0,
      "matrix": []
    }
  },
  "sampling_info": {
    "method": "random",
    "sample_size": 10000,
    "sample_rate": 0.1
  }
}
```

**Response (HTML format):**
Returns an HTML report with interactive visualizations, charts, and detailed statistical analysis.

---

## 5. Sampling API

### Column Information

#### Get Columns
**GET** `/api/sampling/{dataset_id}/{version_id}/columns`

Get column information for sampling.

**Response:**
```json
{
  "columns": [
    {
      "name": "product_category",
      "data_type": "string",
      "nullable": false,
      "unique_count": 25
    },
    {
      "name": "price",
      "data_type": "numeric",
      "nullable": false,
      "min": 0.99,
      "max": 999.99
    }
  ]
}
```

### Multi-Round Sampling

#### Create Sampling Job (Async)
**POST** `/api/sampling/{dataset_id}/{version_id}/multi-round/run`

Create an asynchronous multi-round sampling job.

**Request:**
```json
{
  "rounds": [
    {
      "round_number": 1,
      "method": "random",
      "parameters": {"sample_size": 1000, "seed": 42},
      "output_name": "round_1_random"
    },
    {
      "round_number": 2,
      "method": "stratified",
      "parameters": {
        "strata_columns": ["category"],
        "sample_size": 500
      },
      "output_name": "round_2_stratified"
    }
  ],
  "export_residual": true,
  "residual_output_name": "final_residual"
}
```

**Response:**
```json
{
  "job_id": "job-123-456-789",
  "status": "pending",
  "created_at": "2024-01-25T10:30:00Z",
  "message": "Multi-round sampling job created"
}
```

#### Execute Sampling (Sync)
**POST** `/api/sampling/{dataset_id}/{version_id}/multi-round/execute`

Execute multi-round sampling synchronously.

**Request:** Same as async version

**Response:**
```json
{
  "job_id": "job-123-456-789",
  "status": "completed",
  "rounds": [
    {
      "round_number": 1,
      "method": "stratified",
      "sample_size": 1000,
      "actual_size": 1000,
      "execution_time": 1.23
    },
    {
      "round_number": 2,
      "method": "random",
      "sample_size": 500,
      "actual_size": 500,
      "execution_time": 0.85
    }
  ],
  "total_sample_size": 1500,
  "total_unique_samples": 1485,
  "execution_time": 2.15
}
```

#### Get Job Status
**GET** `/api/sampling/multi-round/jobs/{job_id}`

Check status of a sampling job.

**Response:**
```json
{
  "job_id": "job-123-456-789",
  "status": "running",
  "progress": {
    "current_round": 2,
    "total_rounds": 3,
    "percentage": 66.7
  },
  "created_at": "2024-01-25T10:30:00Z",
  "updated_at": "2024-01-25T10:32:00Z"
}
```

#### Get Merged Sample
**GET** `/api/sampling/multi-round/jobs/{job_id}/merged-sample`

Get the final merged sample data.

**Query Parameters:**
- `format`: Output format (json, csv)
- `page`: Page number
- `page_size`: Rows per page

**Response:**
```json
{
  "data": [
    {
      "id": 1,
      "product": "Widget A",
      "category": "Electronics",
      "price": 199.99,
      "_round": 1,
      "_sample_method": "stratified"
    }
  ],
  "total_rows": 1485,
  "page": 1,
  "page_size": 100
}
```

#### Get Round Preview
**GET** `/api/sampling/multi-round/jobs/{job_id}/round/{round_number}/preview`

Preview data from a specific sampling round.

**Query Parameters:**
- `limit`: Number of rows to preview (default: 100)

**Response:**
```json
{
  "round_number": 1,
  "method": "stratified",
  "sample_size": 1000,
  "preview_data": [
    {
      "id": 1,
      "product": "Widget A",
      "category": "Electronics",
      "price": 199.99
    }
  ],
  "total_rows": 1000
}
```

#### Get Residual Preview
**GET** `/api/sampling/multi-round/jobs/{job_id}/residual/preview`

Preview data not included in any sampling round.

**Query Parameters:**
- `limit`: Number of rows to preview (default: 100)

**Response:**
```json
{
  "residual_size": 5000,
  "preview_data": [
    {
      "id": 1001,
      "product": "Widget Z",
      "category": "Other",
      "price": 9.99
    }
  ]
}
```

### Sampling History

#### Get User Samplings
**GET** `/api/sampling/user/{user_id}/samplings`

Get sampling history for a user.

**Query Parameters:**
- `page`: Page number (default: 1, 1-indexed)
- `page_size`: Number of items per page (default: 10, max: 100)

**Response:**
```json
{
  "runs": [
    {
      "id": 1,
      "run_id": "job-123-456-789",
      "dataset_id": 1,
      "dataset_version_id": 3,
      "user_id": 1,
      "analysis_type": "multi_round_sampling",
      "parameters": {
        "rounds": [...],
        "export_residual": true
      },
      "status": "completed",
      "created_at": "2024-01-25T10:30:00Z",
      "updated_at": "2024-01-25T10:32:00Z",
      "completed_at": "2024-01-25T10:32:00Z",
      "output_info": {
        "total_sample_size": 1500,
        "round_count": 2,
        "residual_exported": true
      }
    }
  ],
  "total_count": 25,
  "page": 1,
  "page_size": 10
}
```

#### Get Samplings by Dataset Version
**GET** `/api/sampling/dataset-version/{dataset_version_id}/samplings`

Get all samplings for a specific dataset version.

**Query Parameters:**
- `page`: Page number (default: 1, 1-indexed)
- `page_size`: Number of items per page (default: 10, max: 100)

**Response:** Same format as Get User Samplings

#### Get Samplings by Dataset
**GET** `/api/sampling/dataset/{dataset_id}/samplings`

Get all samplings across all versions of a dataset.

**Query Parameters:**
- `page`: Page number (default: 1, 1-indexed)
- `page_size`: Number of items per page (default: 10, max: 100)

**Response:** Same format as Get User Samplings

---

## Common Response Codes

- `200 OK`: Successful request
- `201 Created`: Resource created successfully
- `400 Bad Request`: Invalid request parameters
- `401 Unauthorized`: Missing or invalid authentication
- `403 Forbidden`: Insufficient permissions
- `404 Not Found`: Resource not found
- `409 Conflict`: Resource already exists
- `422 Unprocessable Entity`: Validation error
- `500 Internal Server Error`: Server error

## Error Response Format

All error responses follow this format:

```json
{
  "detail": "Error message describing what went wrong",
  "error_code": "SPECIFIC_ERROR_CODE",
  "timestamp": "2024-01-25T10:30:00Z"
}
```

## Rate Limiting

- Default: 100 requests per minute per user
- File uploads: 10 per minute per user
- Large data exports: 5 per minute per user

## Notes

### Authentication
- All endpoints except health checks, login, and user registration require authentication
- Use Bearer token in Authorization header: `Authorization: Bearer <access_token>`
- Access tokens expire after configured time (default from ACCESS_TOKEN_EXPIRE_MINUTES)
- Refresh tokens expire after configured days (default from REFRESH_TOKEN_EXPIRE_DAYS)
- Token refresh endpoint allows obtaining new access token without re-authentication

### File Support
- Supported formats: CSV, XLSX, XLS, XLSM, Parquet
- Files are automatically converted to Parquet format for storage
- No hard file size limit (previously 500MB)
- Multi-sheet Excel files are supported

### Pagination
- Most list endpoints support pagination
- Default page sizes vary by endpoint (typically 10-20 items)
- Maximum page sizes are enforced (typically 100 items)
- Pagination uses either page/page_size or limit/offset patterns

### Timestamps
- All timestamps are in ISO 8601 format (UTC)
- Format: `YYYY-MM-DDTHH:MM:SSZ`
- Example: `2024-01-15T10:30:00Z`

### Permissions
- Dataset permissions: read, write, admin
- Permissions are hierarchical (admin includes all others)
- Dataset creator automatically gets admin permission
- Only admins can grant/revoke permissions

### Search Features
- Full-text search with relevance scoring
- Fuzzy/typo-tolerant search option
- Advanced filtering by multiple criteria
- Faceted search for dynamic filtering
- Search suggestions for autocomplete

### Error Handling
- Consistent error response format across all endpoints
- Detailed error messages for debugging
- HTTP status codes follow REST conventions
- Validation errors return 422 status code