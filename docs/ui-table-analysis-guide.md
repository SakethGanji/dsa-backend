# UI Guide: Table Analysis Endpoint

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

## UI Implementation Guidelines

### 1. Table Overview Section
Display basic table metadata:
```jsx
<TableOverview>
  <h3>{response.table_key}</h3>
  <p>Total Rows: {response.total_rows.toLocaleString()}</p>
  <p>Columns: {response.columns.length}</p>
</TableOverview>
```

### 2. Column Details Grid
Create a comprehensive column information display:
```jsx
<ColumnGrid>
  {response.columns.map(column => (
    <ColumnCard key={column}>
      <h4>{column}</h4>
      <Badge>{response.column_types[column] || 'unknown'}</Badge>
      <Stats>
        <div>Null values: {response.null_counts[column]}</div>
        <div>Null rate: {((response.null_counts[column] / response.total_rows) * 100).toFixed(2)}%</div>
      </Stats>
      <SampleValues values={response.sample_values[column]} />
    </ColumnCard>
  ))}
</ColumnGrid>
```

### 3. Sample Values Display
Show sample values with appropriate formatting:
```jsx
const SampleValues = ({ values }) => {
  if (!values || values.length === 0) {
    return <EmptyState>No sample values available</EmptyState>;
  }
  
  return (
    <SampleContainer>
      <h5>Sample Values ({values.length})</h5>
      <ValueList>
        {values.slice(0, 5).map((value, idx) => (
          <ValueItem key={idx}>{formatValue(value)}</ValueItem>
        ))}
        {values.length > 5 && <MoreIndicator>+{values.length - 5} more</MoreIndicator>}
      </ValueList>
    </SampleContainer>
  );
};
```

### 4. Data Quality Indicators
Visualize data quality metrics:
```jsx
const DataQuality = ({ nullCounts, totalRows, columns }) => {
  const qualityScore = columns.reduce((acc, col) => {
    const nullRate = nullCounts[col] / totalRows;
    return acc + (1 - nullRate);
  }, 0) / columns.length * 100;
  
  return (
    <QualitySection>
      <h4>Data Quality Score: {qualityScore.toFixed(1)}%</h4>
      <ProgressBar value={qualityScore} />
      <QualityBreakdown>
        {columns.map(col => {
          const completeness = ((totalRows - nullCounts[col]) / totalRows * 100);
          return (
            <ColumnQuality key={col}>
              <span>{col}</span>
              <MiniProgressBar value={completeness} />
              <span>{completeness.toFixed(1)}% complete</span>
            </ColumnQuality>
          );
        })}
      </QualityBreakdown>
    </QualitySection>
  );
};
```

### 5. Loading States
Implement proper loading states:
```jsx
const TableAnalysis = ({ datasetId, refName, tableKey }) => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  useEffect(() => {
    fetchTableAnalysis(datasetId, refName, tableKey)
      .then(setData)
      .catch(setError)
      .finally(() => setLoading(false));
  }, [datasetId, refName, tableKey]);
  
  if (loading) return <AnalysisSkeleton />;
  if (error) return <ErrorState error={error} />;
  if (!data) return <EmptyState />;
  
  return <AnalysisDisplay data={data} />;
};
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

### UI Error Handling
```jsx
const handleAnalysisError = (error) => {
  switch (error.code) {
    case 'TABLE_NOT_FOUND':
      return <Alert type="error">The requested table does not exist in this reference.</Alert>;
    case 'PERMISSION_DENIED':
      return <Alert type="error">You don't have permission to view this table's analysis.</Alert>;
    default:
      return <Alert type="error">Failed to load table analysis. Please try again.</Alert>;
  }
};
```

## Performance Considerations

1. **Caching**: Consider caching analysis results client-side for recently viewed tables
2. **Pagination**: For tables with many columns, implement virtual scrolling or pagination
3. **Progressive Loading**: Load basic stats first, then sample values
4. **Debouncing**: If switching between tables rapidly, debounce API calls

## Example Implementation

```jsx
// Complete React component example
const TableAnalysisView = () => {
  const { datasetId, refName, tableKey } = useParams();
  const { data, loading, error } = useTableAnalysis(datasetId, refName, tableKey);
  
  if (loading) return <LoadingSpinner />;
  if (error) return <ErrorDisplay error={error} />;
  
  return (
    <AnalysisContainer>
      <Header>
        <h2>Table Analysis: {data.table_key}</h2>
        <RefreshButton onClick={refetch} />
      </Header>
      
      <MetricsRow>
        <Metric label="Total Rows" value={data.total_rows} />
        <Metric label="Columns" value={data.columns.length} />
        <Metric label="Data Quality" value={calculateQuality(data)} />
      </MetricsRow>
      
      <TabContainer>
        <Tab label="Schema">
          <SchemaView data={data} />
        </Tab>
        <Tab label="Sample Data">
          <SampleDataView data={data} />
        </Tab>
        <Tab label="Statistics">
          <StatisticsView data={data} />
        </Tab>
      </TabContainer>
    </AnalysisContainer>
  );
};
```

## Best Practices

1. **Always show loading states** while fetching analysis data
2. **Handle empty tables gracefully** with appropriate messaging
3. **Format large numbers** with locale-appropriate separators
4. **Provide export options** for analysis results (CSV, JSON)
5. **Use tooltips** to explain statistical terms and metrics
6. **Implement responsive design** for mobile viewing
7. **Cache results** to avoid redundant API calls
8. **Show data freshness** indicators if statistics might be stale