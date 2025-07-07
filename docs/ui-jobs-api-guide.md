# Jobs API Guide for UI

## Overview

There are several job-related endpoints. Here's when to use each one:

## 1. Get Single Job Details

**When to use**: When you have a job ID and want to get its full details (status, parameters, results, etc.)

### Endpoint
```
GET /api/jobs/{job_id}
```

### Example Request
```bash
curl 'http://localhost:8000/api/jobs/917f045a-ec63-47a1-b192-35149508f452' \
  -H 'Authorization: Bearer {token}'
```

### Response
```json
{
  "id": "917f045a-ec63-47a1-b192-35149508f452",
  "run_type": "sampling",
  "status": "completed",
  "dataset_id": 48,
  "dataset_name": "Sales Data 2024",
  "source_commit_id": "abc123",
  "user_id": 87,
  "user_soeid": "bg54677",
  "run_parameters": {
    "rounds": [...],
    "table_key": "primary"
  },
  "output_summary": {...},
  "error_message": null,
  "created_at": "2025-07-07T02:45:47.569440+00:00",
  "completed_at": "2025-07-07T02:45:49.455826+00:00",
  "duration_seconds": 2.5
}
```

## 2. List/Search Jobs

**When to use**: When you want to find jobs based on criteria (user, dataset, status, etc.)

### Endpoint
```
GET /api/jobs
```

### Query Parameters
- `user_id`: Filter by user ID
- `user_soeid`: Filter by user SOEID (e.g., "bg54677")
- `dataset_id`: Filter by dataset ID
- `status`: Filter by status (pending, running, completed, failed)
- `run_type`: Filter by type (sampling, import, exploration, profiling)
- `offset`: Skip N results (for pagination)
- `limit`: Return max N results (default: 100, max: 1000)

### Example Requests

#### Get all jobs for a dataset
```bash
curl 'http://localhost:8000/api/jobs?dataset_id=48' \
  -H 'Authorization: Bearer {token}'
```

#### Get failed jobs for a user
```bash
curl 'http://localhost:8000/api/jobs?user_soeid=bg54677&status=failed' \
  -H 'Authorization: Bearer {token}'
```

#### Get recent sampling jobs
```bash
curl 'http://localhost:8000/api/jobs?run_type=sampling&limit=20' \
  -H 'Authorization: Bearer {token}'
```

### Response
```json
{
  "jobs": [
    {
      "id": "917f045a-ec63-47a1-b192-35149508f452",
      "run_type": "sampling",
      "status": "completed",
      "dataset_id": 48,
      "dataset_name": "Sales Data 2024",
      "user_id": 87,
      "user_soeid": "bg54677",
      "created_at": "2025-07-07T02:45:47.569440+00:00",
      "completed_at": "2025-07-07T02:45:49.455826+00:00",
      "error_message": null
    },
    // ... more jobs
  ],
  "total": 125,
  "offset": 0,
  "limit": 100
}
```

## 3. Create a Sampling Job

**When to use**: When you want to create a new sampling job

### Endpoint
```
POST /api/sampling/datasets/{dataset_id}/jobs
```

### Request Body
```json
{
  "source_ref": "main",
  "table_key": "primary",
  "rounds": [
    {
      "round_number": 1,
      "method": "random",
      "parameters": {
        "sample_size": 1000  // ⚠️ Required!
      }
    }
  ]
}
```

### Response
```json
{
  "job_id": "917f045a-ec63-47a1-b192-35149508f452",
  "status": "pending",
  "message": "Sampling job created with 1 rounds"
}
```

## 4. Get Sampling Job Data

**When to use**: After a sampling job completes, to retrieve the sampled data

### Endpoint
```
GET /api/sampling/jobs/{job_id}/data
```

### Query Parameters
- `table_key`: Which table to retrieve (default: "primary")
- `offset`: Skip N rows
- `limit`: Return max N rows
- `columns`: Comma-separated column names
- `format`: Output format ("json" or "csv")

### Example
```bash
curl 'http://localhost:8000/api/sampling/jobs/917f045a-ec63-47a1-b192-35149508f452/data?limit=100' \
  -H 'Authorization: Bearer {token}'
```

## Common UI Workflows

### 1. Creating and Monitoring a Sampling Job

```javascript
// Step 1: Create the job
const createResponse = await fetch(`/api/sampling/datasets/${datasetId}/jobs`, {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${token}`
  },
  body: JSON.stringify({
    rounds: [{
      round_number: 1,
      method: "random",
      parameters: { sample_size: 1000 }
    }]
  })
});

const { job_id } = await createResponse.json();

// Step 2: Poll for job status
const pollJobStatus = async (jobId) => {
  const response = await fetch(`/api/jobs/${jobId}`, {
    headers: { 'Authorization': `Bearer ${token}` }
  });
  
  const job = await response.json();
  
  if (job.status === 'completed') {
    // Job done - fetch the data
    const dataResponse = await fetch(`/api/sampling/jobs/${jobId}/data`, {
      headers: { 'Authorization': `Bearer ${token}` }
    });
    return await dataResponse.json();
  } else if (job.status === 'failed') {
    throw new Error(job.error_message);
  } else {
    // Still running - poll again in a few seconds
    setTimeout(() => pollJobStatus(jobId), 5000);
  }
};
```

### 2. Displaying Job History for a Dataset

```javascript
// Get all sampling jobs for a dataset
const response = await fetch(`/api/jobs?dataset_id=${datasetId}&run_type=sampling`, {
  headers: { 'Authorization': `Bearer ${token}` }
});

const { jobs } = await response.json();

// Display in a table
jobs.forEach(job => {
  console.log(`Job ${job.id}: ${job.status} - Created ${job.created_at}`);
});
```

### 3. Error Handling

```javascript
// When checking job status
const response = await fetch(`/api/jobs/${jobId}`, {
  headers: { 'Authorization': `Bearer ${token}` }
});

if (!response.ok) {
  if (response.status === 404) {
    console.error("Job not found");
  } else if (response.status === 401) {
    console.error("Authentication required");
  } else {
    console.error(`Error: ${response.statusText}`);
  }
  return;
}

const job = await response.json();
if (job.status === 'failed') {
  console.error(`Job failed: ${job.error_message}`);
  // Show error to user
}
```

## API Response Status Codes

| Code | Meaning | Action |
|------|---------|--------|
| 200 | Success | Process response |
| 401 | Unauthorized | Redirect to login |
| 404 | Job not found | Show "job not found" message |
| 422 | Invalid parameters | Show validation errors |
| 500 | Server error | Show generic error message |

## Tips for UI Implementation

1. **After creating a job**, store the `job_id` and poll `/api/jobs/{job_id}` for status updates
2. **Don't poll too frequently** - every 5-10 seconds is reasonable
3. **Show progress indicators** while jobs are running
4. **Cache job results** to avoid unnecessary API calls
5. **Use the list endpoint** for showing job history, not individual job endpoints
6. **Handle pagination** when displaying job lists
7. **Show meaningful error messages** from the `error_message` field when jobs fail