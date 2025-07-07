# Sampling Endpoints Design Document

## Overview
This document outlines the design for new sampling endpoints that enable retrieving sampled data from commits and tracking sampling history. The design follows existing patterns in the codebase and adheres to DRY principles.

## Key Design Principles
1. **Reuse existing interfaces** - Leverage ITableReader, IJobRepository, and ICommitRepository
2. **Follow existing patterns** - Maintain consistency with current endpoint structure
3. **Store samples as commits** - All sampled data is saved as new commits with metadata
4. **Support async operations** - UI can poll job status and retrieve results when complete

## Endpoint Specifications

### 1. Get Sampling Job Data
Retrieves the sampled data from a completed sampling job.

**Endpoint:** `GET /api/sampling/jobs/{job_id}/data`

**Headers:**
```json
{
  "Accept": "application/json",
  "Authorization": "Bearer {access_token}"
}
```

**Query Parameters:**
- `table_key` (optional): Specific table to retrieve (default: "primary")
- `offset` (optional): Pagination offset (default: 0)
- `limit` (optional): Items per page (default: 100, max: 1000)
- `columns` (optional): Specific columns to retrieve (comma-separated)
- `format` (optional): Export format ("json" or "csv", default: "json")

**Response (JSON format):**
```json
{
  "job_id": "3981ef97-7e8a-4cb3-ba7f-81bf749f5c8d",
  "dataset_id": 123,
  "commit_id": "20250106_sampling_a70ce382",
  "table_key": "primary",
  "data": [
    {
      "logical_row_id": "row_001",
      "data": {
        "id": 1,
        "name": "John Doe",
        "region": "North",
        "value": 1500.50,
        "_sampling_round": 1,
        "_sampling_method": "stratified"
      }
    },
    {
      "logical_row_id": "row_045", 
      "data": {
        "id": 45,
        "name": "Jane Smith",
        "region": "South",
        "value": 2300.75,
        "_sampling_round": 2,
        "_sampling_method": "random"
      }
    }
  ],
  "pagination": {
    "offset": 0,
    "limit": 100,
    "total_rows": 1500,
    "has_more": true
  },
  "metadata": {
    "sampling_summary": {
      "total_samples": 1500,
      "rounds_completed": 3,
      "methods_used": ["stratified", "random", "systematic"],
      "sampling_date": "2024-01-15T10:30:00Z"
    },
    "round_details": [
      {
        "round_number": 1,
        "method": "stratified",
        "rows_sampled": 800,
        "parameters": {
          "sample_size": 800,
          "strata_columns": ["region"],
          "min_per_stratum": 100
        }
      },
      {
        "round_number": 2,
        "method": "random",
        "rows_sampled": 500,
        "parameters": {
          "sample_size": 500,
          "seed": 42
        }
      }
    ],
    "residual_info": {
      "has_residual": true,
      "residual_count": 8500,
      "residual_commit_id": "20250106_residual_b81df493"
    }
  },
  "columns": ["id", "name", "region", "value", "_sampling_round", "_sampling_method"]
}
```

**Response (CSV format):**
```
Content-Type: text/csv
Content-Disposition: attachment; filename="sampling_job_3981ef97_export.csv"

id,name,region,value,_sampling_round,_sampling_method
1,"John Doe",North,1500.50,1,stratified
45,"Jane Smith",South,2300.75,2,random
...
```

**Error Responses:**
- `404 Not Found` - Job not found or user lacks permission
- `400 Bad Request` - Job not completed or failed
- `403 Forbidden` - User lacks permission to access dataset

### 2. Get Sampling History for Dataset
Retrieves historical sampling jobs for a specific dataset.

**Endpoint:** `GET /api/sampling/datasets/{dataset_id}/history`

**Headers:**
```json
{
  "Accept": "application/json", 
  "Authorization": "Bearer {access_token}"
}
```

**Query Parameters:**
- `ref_name` (optional): Filter by specific ref (e.g., "main")
- `offset` (optional): Pagination offset (default: 0)
- `limit` (optional): Items per page (default: 20, max: 100)
- `status` (optional): Filter by job status ("completed", "failed", "running", "pending")
- `start_date` (optional): Filter jobs created after this date (ISO 8601)
- `end_date` (optional): Filter jobs created before this date (ISO 8601)

**Response:**
```json
{
  "dataset_id": 123,
  "dataset_name": "Customer Data 2024",
  "jobs": [
    {
      "job_id": "3981ef97-7e8a-4cb3-ba7f-81bf749f5c8d",
      "status": "completed",
      "created_at": "2024-01-15T10:25:00Z",
      "completed_at": "2024-01-15T10:30:00Z",
      "duration_seconds": 300,
      "created_by": {
        "user_id": 789,
        "soeid": "bg54677",
        "name": "John Doe"
      },
      "source_ref": "main",
      "source_commit_id": "20250115_main_xyz123",
      "output_commit_id": "20250115_sampling_a70ce382",
      "sampling_summary": {
        "total_rounds": 3,
        "total_samples": 1500,
        "methods_used": ["stratified", "random", "systematic"],
        "table_keys_sampled": ["primary"],
        "has_residual": true
      },
      "commit_message": "Stratified sampling for Q1 analysis"
    },
    {
      "job_id": "2871de86-6d7b-4bc2-aa6e-70ae648e4b7b",
      "status": "failed",
      "created_at": "2024-01-14T15:20:00Z",
      "completed_at": "2024-01-14T15:21:00Z",
      "duration_seconds": 60,
      "created_by": {
        "user_id": 456,
        "soeid": "jd12345",
        "name": "Jane Smith"
      },
      "source_ref": "dev",
      "source_commit_id": "20250114_dev_abc456",
      "output_commit_id": null,
      "error_message": "Column 'category' not found for stratified sampling",
      "sampling_summary": null
    }
  ],
  "pagination": {
    "offset": 0,
    "limit": 20,
    "total_jobs": 45,
    "has_more": true
  }
}
```

### 3. Get User's Sampling History
Retrieves all sampling jobs created by a specific user.

**Endpoint:** `GET /api/sampling/users/{user_id}/history`

**Headers:**
```json
{
  "Accept": "application/json",
  "Authorization": "Bearer {access_token}"
}
```

**Query Parameters:**
- `offset` (optional): Pagination offset (default: 0)
- `limit` (optional): Items per page (default: 20, max: 100)
- `dataset_id` (optional): Filter by specific dataset
- `status` (optional): Filter by job status
- `start_date` (optional): Filter jobs created after this date
- `end_date` (optional): Filter jobs created before this date

**Response:**
```json
{
  "user_id": 789,
  "user_soeid": "bg54677",
  "user_name": "John Doe",
  "jobs": [
    {
      "job_id": "3981ef97-7e8a-4cb3-ba7f-81bf749f5c8d",
      "dataset_id": 123,
      "dataset_name": "Customer Data 2024",
      "status": "completed",
      "created_at": "2024-01-15T10:25:00Z",
      "completed_at": "2024-01-15T10:30:00Z",
      "duration_seconds": 300,
      "source_ref": "main",
      "sampling_summary": {
        "total_rounds": 3,
        "total_samples": 1500,
        "methods_used": ["stratified", "random", "systematic"]
      }
    }
  ],
  "pagination": {
    "offset": 0,
    "limit": 20,
    "total_jobs": 12,
    "has_more": false
  },
  "summary": {
    "total_sampling_jobs": 12,
    "successful_jobs": 10,
    "failed_jobs": 2,
    "total_rows_sampled": 15000,
    "datasets_sampled": 5
  }
}
```

### 4. Get Sampling Job Residual Data
Retrieves the unsampled (residual) data from a sampling job.

**Endpoint:** `GET /api/sampling/jobs/{job_id}/residual`

**Headers:**
```json
{
  "Accept": "application/json",
  "Authorization": "Bearer {access_token}"
}
```

**Query Parameters:**
- Same as Get Sampling Job Data endpoint

**Response:**
- Same structure as Get Sampling Job Data but with residual records
- Only available if job was created with `export_residual: true`

## Implementation Details

### Handler Structure
Following the vertical slice architecture:

```python
# src/features/sampling/get_job_data.py
class GetSamplingJobDataHandler(BaseHandler[Dict[str, Any]], PaginationMixin):
    def __init__(self, uow: IUnitOfWork, table_reader: ITableReader):
        super().__init__(uow)
        self._table_reader = table_reader
    
    @with_error_handling
    async def handle(self, job_id: str, table_key: str = "primary", 
                    offset: int = 0, limit: int = 100, 
                    columns: Optional[List[str]] = None,
                    format: str = "json") -> Dict[str, Any]:
        # 1. Get job details from repository
        # 2. Extract output_commit_id from job.output_summary
        # 3. Use table_reader to get commit data
        # 4. Enrich with sampling metadata
        # 5. Return paginated response or CSV stream
```

### Repository Extensions
Add methods to IJobRepository (sampling IS a type of job, so it belongs here):

```python
async def get_sampling_jobs_by_dataset(
    self, dataset_id: int, ref_name: Optional[str] = None,
    status: Optional[str] = None, offset: int = 0, limit: int = 20
) -> Tuple[List[Job], int]

async def get_sampling_jobs_by_user(
    self, user_id: int, dataset_id: Optional[int] = None,
    status: Optional[str] = None, offset: int = 0, limit: int = 20
) -> Tuple[List[Job], int]
```

### API Route Registration

```python
# src/api/sampling.py
@router.get("/jobs/{job_id}/data")
async def get_sampling_job_data(
    job_id: str,
    table_key: str = Query("primary"),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    columns: Optional[str] = Query(None),
    format: Optional[str] = Query("json"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: IUnitOfWork = Depends(get_unit_of_work),
    table_reader: ITableReader = Depends(get_table_reader)
):
    handler = GetSamplingJobDataHandler(uow, table_reader)
    return await handler.handle(job_id, table_key, offset, limit, columns)
```

## UI Integration Flow

1. **Create sampling job**
   ```
   POST /api/sampling/datasets/{dataset_id}/jobs
   Response: { "job_id": "...", "status": "pending" }
   ```

2. **Poll job status**
   ```
   GET /api/jobs/{job_id}
   Response: { "status": "running" | "completed" | "failed", ... }
   ```

3. **Retrieve sampled data** (when status = "completed")
   ```
   GET /api/sampling/jobs/{job_id}/data
   Response: { paginated sample data with metadata }
   ```

4. **Export data** (optional)
   ```
   GET /api/sampling/jobs/{job_id}/data?format=csv
   Response: CSV file download
   ```

## Database Schema (Existing)

No schema changes required. The implementation leverages:

- `dsa_jobs.analysis_runs` - Stores job details and output_summary
- `dsa_core.commits` - Stores sampled data as commits
- `dsa_core.commit_rows` - Links commits to row data
- `dsa_core.commit_statistics` - Stores sampling metadata

## Security Considerations

1. **Authorization**: All endpoints check dataset read permissions
2. **User isolation**: Users can only see their own sampling history (unless admin)
3. **Dataset access**: Job data access requires permission to the underlying dataset
4. **Audit trail**: All sampling operations are logged with user and timestamp

## Performance Considerations

1. **Pagination**: All data endpoints support pagination to handle large datasets
2. **Streaming**: Large exports can use table_reader's streaming capabilities
3. **Caching**: Consider caching job metadata for frequently accessed jobs
4. **Indexes**: Ensure indexes on job.dataset_id, job.user_id for history queries
5. **Hash-based sampling**: Uses efficient hash filtering that scales to billions of rows
6. **SQL-based processing**: All sampling operations happen in PostgreSQL for optimal performance

## Implementation Status

### Completed
- ✅ Core sampling API (`POST /sampling/datasets/{dataset_id}/jobs`)
- ✅ Direct sampling endpoint (`POST /sampling/datasets/{dataset_id}/sample`)
- ✅ Column samples endpoint (`POST /sampling/datasets/{dataset_id}/column-samples`)
- ✅ Sampling methods info endpoint (`GET /sampling/datasets/{dataset_id}/sampling-methods`)
- ✅ SQL-based sampling executor with multiple strategies
- ✅ Sampling service with streaming support
- ✅ Database indexes for performance

### Newly Implemented
- ✅ Get sampling job data endpoint (`GET /sampling/jobs/{job_id}/data`)
- ✅ Get sampling history for dataset (`GET /sampling/datasets/{dataset_id}/history`)
- ✅ Get user's sampling history (`GET /sampling/users/{user_id}/history`)
- ✅ Get sampling job residual data (`GET /sampling/jobs/{job_id}/residual`)
- ✅ CSV export functionality for data endpoints
- ✅ Repository extensions for sampling job queries

### Implementation Notes
1. All endpoints follow the existing handler pattern with proper error handling
2. CSV export is implemented using FastAPI's StreamingResponse for efficient memory usage
3. Repository methods use dynamic SQL building with proper parameterization to prevent SQL injection
4. Permission checks ensure users can only access data they have rights to
5. The implementation properly reuses existing interfaces (ITableReader, IJobRepository, IUnitOfWork)