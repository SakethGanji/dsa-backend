# UI Integration Guide: Dataset Overview and Columns Endpoints

## Table of Contents
1. [Overview](#overview)
2. [The Problem](#the-problem)
3. [The Solution](#the-solution)
4. [API Endpoints](#api-endpoints)
5. [Integration Examples](#integration-examples)
6. [Best Practices](#best-practices)
7. [Error Handling](#error-handling)

## Overview

This guide explains how to integrate the Dataset Overview endpoint with the Columns endpoint to create a seamless UI experience for selecting and viewing dataset columns.

## The Problem

The columns endpoint requires two parameters:
- `ref_name`: The branch/ref name (e.g., "main", "feature-branch")
- `table_key`: The table identifier (e.g., "Sheet1", "customers", "orders")

Previously, the UI had no way to know what valid values to use for these parameters without making multiple API calls or hardcoding values.

## The Solution

The new Dataset Overview endpoint (`GET /datasets/{dataset_id}/overview`) provides all refs and tables in a single API call, making it easy to populate UI dropdowns and validate user selections.

## API Endpoints

### 1. Dataset Overview Endpoint

**Endpoint**: `GET /api/datasets/{dataset_id}/overview`

**Purpose**: Get all branches (refs) and their associated tables for a dataset

**Authentication**: Bearer token required

**Response Structure**:
```json
{
  "dataset_id": 42,
  "dataset_name": "Sales Data 2024",
  "default_ref": "main",
  "refs": [
    {
      "ref_name": "main",
      "commit_id": "abc123...",
      "created_at": "2024-01-01T00:00:00Z",
      "updated_at": "2024-01-02T00:00:00Z",
      "tables": [
        {
          "table_key": "orders",
          "row_count": 1500,
          "column_count": 12
        },
        {
          "table_key": "customers",
          "row_count": 500,
          "column_count": 8
        }
      ]
    },
    {
      "ref_name": "feature-new-fields",
      "commit_id": "def456...",
      "created_at": "2024-01-03T00:00:00Z",
      "updated_at": "2024-01-03T00:00:00Z",
      "tables": [
        {
          "table_key": "orders",
          "row_count": 1600,
          "column_count": 15
        }
      ]
    }
  ]
}
```

### 2. Columns Endpoint

**Endpoint**: `GET /api/datasets/{dataset_id}/refs/{ref_name}/tables/{table_key}/columns`

**Purpose**: Get column information for a specific table

**Authentication**: Bearer token required

**Query Parameters** (optional):
- `include_stats=true`: Include column statistics
- `search=user`: Search columns by name
- `type=string`: Filter by data type

**Response Structure**:
```json
{
  "columns": [
    {
      "name": "order_id",
      "type": "integer",
      "nullable": false
    },
    {
      "name": "customer_email",
      "type": "string",
      "nullable": true
    }
  ],
  "total_count": 12
}
```

## Integration Examples

### React Example

```jsx
import React, { useState, useEffect } from 'react';
import axios from 'axios';

function DatasetColumnSelector({ datasetId, authToken }) {
  const [overview, setOverview] = useState(null);
  const [selectedRef, setSelectedRef] = useState('');
  const [selectedTable, setSelectedTable] = useState('');
  const [columns, setColumns] = useState([]);
  const [loading, setLoading] = useState(false);

  // Fetch dataset overview on component mount
  useEffect(() => {
    fetchOverview();
  }, [datasetId]);

  // Fetch columns when table selection changes
  useEffect(() => {
    if (selectedRef && selectedTable) {
      fetchColumns();
    }
  }, [selectedRef, selectedTable]);

  const fetchOverview = async () => {
    try {
      const response = await axios.get(
        `/api/datasets/${datasetId}/overview`,
        { headers: { Authorization: `Bearer ${authToken}` } }
      );
      setOverview(response.data);
      
      // Auto-select default ref
      if (response.data.default_ref) {
        setSelectedRef(response.data.default_ref);
      }
    } catch (error) {
      console.error('Failed to fetch dataset overview:', error);
    }
  };

  const fetchColumns = async () => {
    setLoading(true);
    try {
      const response = await axios.get(
        `/api/datasets/${datasetId}/refs/${selectedRef}/tables/${selectedTable}/columns`,
        { headers: { Authorization: `Bearer ${authToken}` } }
      );
      setColumns(response.data.columns);
    } catch (error) {
      console.error('Failed to fetch columns:', error);
    } finally {
      setLoading(false);
    }
  };

  const getTablesForRef = () => {
    if (!overview || !selectedRef) return [];
    const ref = overview.refs.find(r => r.ref_name === selectedRef);
    return ref ? ref.tables : [];
  };

  return (
    <div className="dataset-column-selector">
      <h3>Select Data Source</h3>
      
      {/* Branch/Ref Selector */}
      <div className="form-group">
        <label>Branch:</label>
        <select 
          value={selectedRef} 
          onChange={(e) => {
            setSelectedRef(e.target.value);
            setSelectedTable(''); // Reset table selection
          }}
        >
          <option value="">Select a branch...</option>
          {overview?.refs.map(ref => (
            <option key={ref.ref_name} value={ref.ref_name}>
              {ref.ref_name} 
              {ref.ref_name === overview.default_ref && ' (default)'}
            </option>
          ))}
        </select>
      </div>

      {/* Table Selector */}
      <div className="form-group">
        <label>Table:</label>
        <select 
          value={selectedTable} 
          onChange={(e) => setSelectedTable(e.target.value)}
          disabled={!selectedRef}
        >
          <option value="">Select a table...</option>
          {getTablesForRef().map(table => (
            <option key={table.table_key} value={table.table_key}>
              {table.table_key} ({table.row_count} rows, {table.column_count || '?'} columns)
            </option>
          ))}
        </select>
      </div>

      {/* Column List */}
      {loading && <div>Loading columns...</div>}
      {!loading && columns.length > 0 && (
        <div className="column-list">
          <h4>Available Columns:</h4>
          <ul>
            {columns.map(col => (
              <li key={col.name}>
                <strong>{col.name}</strong> ({col.type})
                {col.nullable && ' - nullable'}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
```

### Vue.js Example

```vue
<template>
  <div class="dataset-column-selector">
    <h3>Dataset: {{ overview?.dataset_name }}</h3>
    
    <!-- Branch Selector -->
    <div class="form-group">
      <label>Branch:</label>
      <select v-model="selectedRef" @change="onRefChange">
        <option value="">Select branch...</option>
        <option 
          v-for="ref in refs" 
          :key="ref.ref_name"
          :value="ref.ref_name"
        >
          {{ ref.ref_name }}
          <span v-if="ref.ref_name === overview?.default_ref">(default)</span>
        </option>
      </select>
    </div>

    <!-- Table Selector -->
    <div class="form-group">
      <label>Table:</label>
      <select v-model="selectedTable" :disabled="!selectedRef">
        <option value="">Select table...</option>
        <option 
          v-for="table in availableTables" 
          :key="table.table_key"
          :value="table.table_key"
        >
          {{ table.table_key }} ({{ table.row_count }} rows)
        </option>
      </select>
    </div>

    <!-- Column Display -->
    <div v-if="columns.length > 0" class="columns">
      <h4>Columns:</h4>
      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Type</th>
            <th>Nullable</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="col in columns" :key="col.name">
            <td>{{ col.name }}</td>
            <td>{{ col.type }}</td>
            <td>{{ col.nullable ? 'Yes' : 'No' }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script>
export default {
  props: ['datasetId', 'authToken'],
  
  data() {
    return {
      overview: null,
      selectedRef: '',
      selectedTable: '',
      columns: []
    };
  },
  
  computed: {
    refs() {
      return this.overview?.refs || [];
    },
    
    availableTables() {
      if (!this.selectedRef || !this.overview) return [];
      const ref = this.overview.refs.find(r => r.ref_name === this.selectedRef);
      return ref?.tables || [];
    }
  },
  
  watch: {
    selectedTable(newVal) {
      if (newVal && this.selectedRef) {
        this.fetchColumns();
      }
    }
  },
  
  mounted() {
    this.fetchOverview();
  },
  
  methods: {
    async fetchOverview() {
      try {
        const response = await fetch(`/api/datasets/${this.datasetId}/overview`, {
          headers: { 'Authorization': `Bearer ${this.authToken}` }
        });
        this.overview = await response.json();
        
        // Auto-select default ref
        if (this.overview.default_ref) {
          this.selectedRef = this.overview.default_ref;
        }
      } catch (error) {
        console.error('Failed to fetch overview:', error);
      }
    },
    
    async fetchColumns() {
      try {
        const url = `/api/datasets/${this.datasetId}/refs/${this.selectedRef}/tables/${this.selectedTable}/columns`;
        const response = await fetch(url, {
          headers: { 'Authorization': `Bearer ${this.authToken}` }
        });
        const data = await response.json();
        this.columns = data.columns;
      } catch (error) {
        console.error('Failed to fetch columns:', error);
      }
    },
    
    onRefChange() {
      this.selectedTable = '';
      this.columns = [];
    }
  }
};
</script>
```

### TypeScript Types

```typescript
// API Response Types
interface DatasetOverviewResponse {
  dataset_id: number;
  dataset_name: string;
  default_ref: string;
  refs: RefWithTables[];
}

interface RefWithTables {
  ref_name: string;
  commit_id: string | null;
  created_at: string;
  updated_at: string;
  tables: TableInfo[];
}

interface TableInfo {
  table_key: string;
  row_count: number | null;
  column_count: number | null;
}

interface ColumnsResponse {
  columns: ColumnInfo[];
  total_count: number;
}

interface ColumnInfo {
  name: string;
  type: string;
  nullable: boolean;
}

// Service Class Example
class DatasetService {
  constructor(private apiBase: string, private authToken: string) {}

  async getOverview(datasetId: number): Promise<DatasetOverviewResponse> {
    const response = await fetch(`${this.apiBase}/datasets/${datasetId}/overview`, {
      headers: { 'Authorization': `Bearer ${this.authToken}` }
    });
    
    if (!response.ok) {
      throw new Error(`Failed to fetch overview: ${response.statusText}`);
    }
    
    return response.json();
  }

  async getColumns(
    datasetId: number, 
    refName: string, 
    tableKey: string,
    includeStats: boolean = false
  ): Promise<ColumnsResponse> {
    const params = new URLSearchParams();
    if (includeStats) params.append('include_stats', 'true');
    
    const url = `${this.apiBase}/datasets/${datasetId}/refs/${refName}/tables/${tableKey}/columns?${params}`;
    const response = await fetch(url, {
      headers: { 'Authorization': `Bearer ${this.authToken}` }
    });
    
    if (!response.ok) {
      throw new Error(`Failed to fetch columns: ${response.statusText}`);
    }
    
    return response.json();
  }
}
```

## Best Practices

### 1. Caching Strategy

Cache the overview response since refs and tables don't change frequently:

```javascript
class DatasetCache {
  constructor(ttl = 5 * 60 * 1000) { // 5 minutes default
    this.cache = new Map();
    this.ttl = ttl;
  }

  set(datasetId, data) {
    this.cache.set(datasetId, {
      data,
      timestamp: Date.now()
    });
  }

  get(datasetId) {
    const entry = this.cache.get(datasetId);
    if (!entry) return null;
    
    if (Date.now() - entry.timestamp > this.ttl) {
      this.cache.delete(datasetId);
      return null;
    }
    
    return entry.data;
  }
}

// Usage
const overviewCache = new DatasetCache();

async function getDatasetOverview(datasetId) {
  // Check cache first
  let overview = overviewCache.get(datasetId);
  
  if (!overview) {
    overview = await fetchOverviewFromAPI(datasetId);
    overviewCache.set(datasetId, overview);
  }
  
  return overview;
}
```

### 2. Loading States

Always show loading states for better UX:

```jsx
function ColumnSelector() {
  const [loadingStates, setLoadingStates] = useState({
    overview: false,
    columns: false
  });

  return (
    <div>
      {loadingStates.overview && <Spinner text="Loading dataset info..." />}
      {loadingStates.columns && <Spinner text="Loading columns..." />}
      {/* Rest of component */}
    </div>
  );
}
```

### 3. Smart Defaults

Auto-select sensible defaults to reduce clicks:

```javascript
// After loading overview
if (overview.refs.length === 1) {
  // Only one ref, auto-select it
  setSelectedRef(overview.refs[0].ref_name);
} else if (overview.default_ref) {
  // Multiple refs, select the default
  setSelectedRef(overview.default_ref);
}

// After selecting a ref
const tables = getTablesForRef(selectedRef);
if (tables.length === 1) {
  // Only one table, auto-select it
  setSelectedTable(tables[0].table_key);
}
```

### 4. Handle Multi-Sheet Files

For Excel files with multiple sheets, show sheet names clearly:

```jsx
function TableSelector({ tables }) {
  const isExcel = tables.some(t => 
    ['Sheet1', 'Sheet2'].some(s => t.table_key.includes(s))
  );

  return (
    <select>
      <option value="">
        {isExcel ? 'Select a sheet...' : 'Select a table...'}
      </option>
      {tables.map(table => (
        <option key={table.table_key} value={table.table_key}>
          {isExcel ? `📊 ${table.table_key}` : table.table_key}
          {' '}({table.row_count} rows)
        </option>
      ))}
    </select>
  );
}
```

## Error Handling

### Common Error Scenarios

1. **Dataset Not Found (404)**
```javascript
try {
  const overview = await fetchOverview(datasetId);
} catch (error) {
  if (error.response?.status === 404) {
    showError('Dataset not found. It may have been deleted.');
  }
}
```

2. **Unauthorized (401/403)**
```javascript
if (error.response?.status === 401) {
  // Token expired
  redirectToLogin();
} else if (error.response?.status === 403) {
  showError('You do not have permission to view this dataset.');
}
```

3. **No Tables Available**
```javascript
if (overview.refs.every(ref => ref.tables.length === 0)) {
  showWarning('This dataset has no data yet. Try importing a file first.');
}
```

### Graceful Degradation

Always provide fallbacks:

```jsx
function ColumnList({ columns, error }) {
  if (error) {
    return (
      <div className="error-state">
        <p>Unable to load columns</p>
        <button onClick={retry}>Try Again</button>
      </div>
    );
  }

  if (columns.length === 0) {
    return <p>No columns found in this table.</p>;
  }

  return <ColumnTable columns={columns} />;
}
```

## Complete UI Flow Example

```javascript
// Complete flow from dataset selection to column display
async function initializeDatasetView(datasetId) {
  try {
    // Step 1: Get overview
    const overview = await datasetService.getOverview(datasetId);
    
    // Step 2: Set up UI with branches
    populateBranchDropdown(overview.refs);
    
    // Step 3: Auto-select default branch
    if (overview.default_ref) {
      selectBranch(overview.default_ref);
      
      // Step 4: Get tables for selected branch
      const ref = overview.refs.find(r => r.ref_name === overview.default_ref);
      if (ref && ref.tables.length > 0) {
        populateTableDropdown(ref.tables);
        
        // Step 5: Auto-select first table if only one exists
        if (ref.tables.length === 1) {
          selectTable(ref.tables[0].table_key);
          
          // Step 6: Load columns
          const columns = await datasetService.getColumns(
            datasetId,
            overview.default_ref,
            ref.tables[0].table_key
          );
          
          displayColumns(columns);
        }
      }
    }
  } catch (error) {
    handleError(error);
  }
}
```

## Performance Tips

1. **Batch Requests**: If you need overview for multiple datasets, consider creating a batch endpoint
2. **Lazy Loading**: Only fetch columns when user selects a table
3. **Prefetching**: When user hovers over a table, prefetch its columns
4. **WebSocket Updates**: For real-time updates when new commits are made

## Summary

The Dataset Overview endpoint eliminates the guesswork in selecting refs and tables. By following this guide, your UI can provide a smooth, intuitive experience for users exploring dataset contents.