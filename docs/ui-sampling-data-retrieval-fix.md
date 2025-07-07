# Sampling Data Retrieval Fix

## The Problem

Getting error: `{"detail":"Table 'primary' not found in output"}` when trying to retrieve sampled data.

## Root Cause

The sampling job was created with `table_key: "Sales"`, but the data retrieval request is using the default `table_key: "primary"`.

### Job Parameters (from job details):
```json
{
  "table_key": "Sales",  // ← Job sampled from "Sales" table
  ...
}
```

### Data Retrieval Request:
```
GET /api/sampling/jobs/{job_id}/data?offset=0&limit=50
// No table_key specified, defaults to "primary" ← MISMATCH!
```

## The Fix

### Option 1: Specify the Correct Table Key

Include the `table_key` parameter in the data retrieval request:

```bash
# ✅ Correct - specifies the Sales table
curl 'http://localhost:8000/api/sampling/jobs/e74b1e92-9da7-42d6-acd6-3d1a77992701/data?table_key=Sales&offset=0&limit=50' \
  -H 'Authorization: Bearer {token}'
```

### Option 2: Always Use "primary" Table Key

When creating sampling jobs, always use `table_key: "primary"` (the default):

```json
{
  "source_ref": "main",
  "table_key": "primary",  // ← Use "primary" instead of "Sales"
  "rounds": [...]
}
```

## JavaScript Implementation

### Retrieving Data After Job Completion

```javascript
async function getSamplingData(jobId, jobDetails) {
  // Extract the table_key from the job parameters
  const tableKey = jobDetails.run_parameters.table_key || 'primary';
  
  // Include table_key in the request
  const response = await fetch(
    `/api/sampling/jobs/${jobId}/data?table_key=${tableKey}&offset=0&limit=100`,
    {
      headers: {
        'Authorization': `Bearer ${getAuthToken()}`
      }
    }
  );
  
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail);
  }
  
  return await response.json();
}

// Complete flow
async function completeSamplingFlow(datasetId, tableKey = 'primary') {
  // 1. Create job with specific table
  const createResponse = await fetch(`/api/sampling/datasets/${datasetId}/jobs`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${getAuthToken()}`
    },
    body: JSON.stringify({
      table_key: tableKey,  // Specify which table to sample
      rounds: [{
        round_number: 1,
        method: 'random',
        parameters: { sample_size: 1000 }
      }]
    })
  });
  
  const { job_id } = await createResponse.json();
  
  // 2. Wait for completion (poll job status)
  const jobDetails = await waitForJobCompletion(job_id);
  
  // 3. Retrieve data using the SAME table_key
  const data = await getSamplingData(job_id, jobDetails);
  
  return data;
}
```

## React Component Fix

```jsx
function SamplingResults({ jobId }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  useEffect(() => {
    async function fetchData() {
      try {
        // First get job details to find the table_key
        const jobResponse = await fetch(`/api/jobs/${jobId}`, {
          headers: { 'Authorization': `Bearer ${getAuthToken()}` }
        });
        
        if (!jobResponse.ok) throw new Error('Failed to get job details');
        
        const job = await jobResponse.json();
        
        // Extract table_key from job parameters
        const tableKey = job.run_parameters?.table_key || 'primary';
        
        // Get data with correct table_key
        const dataResponse = await fetch(
          `/api/sampling/jobs/${jobId}/data?table_key=${tableKey}&offset=0&limit=100`,
          {
            headers: { 'Authorization': `Bearer ${getAuthToken()}` }
          }
        );
        
        if (!dataResponse.ok) {
          const error = await dataResponse.json();
          throw new Error(error.detail);
        }
        
        const result = await dataResponse.json();
        setData(result);
        
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    }
    
    fetchData();
  }, [jobId]);
  
  if (loading) return <div>Loading...</div>;
  if (error) return <div>Error: {error}</div>;
  
  return (
    <div>
      <h3>Sampled Data from {data.table_key} table</h3>
      <p>Total rows: {data.pagination.total}</p>
      {/* Display data */}
    </div>
  );
}
```

## URL Fix

Also note that your URL has a typo:
```
# ❌ Wrong - has "^&" 
?offset=0^&limit=50

# ✅ Correct - just "&"
?offset=0&limit=50
```

## Best Practices

1. **Be consistent with table naming**: Either always use "primary" or track which table was sampled
2. **Store table_key in your UI state** when creating jobs
3. **Pass table_key to data retrieval** requests
4. **Handle multi-table datasets** by letting users select which table to sample

## Quick Test

Test the fix with curl:
```bash
# This should work now
curl 'http://localhost:8000/api/sampling/jobs/e74b1e92-9da7-42d6-acd6-3d1a77992701/data?table_key=Sales&offset=0&limit=50' \
  -H 'Authorization: Bearer YOUR_TOKEN'
```