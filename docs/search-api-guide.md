# Search API Guide

This guide explains how to use the advanced search functionality in the DSA Platform.

## Overview

The search API provides:
- Full-text search with fuzzy matching
- Advanced filtering by tags, creator, and dates
- Faceted search for discovering filter options
- Autocomplete suggestions
- Special query syntax for power users

## Endpoints

### 1. Search Datasets

**Endpoint:** `GET /api/datasets/search`

Search for datasets with advanced filtering and faceting.

#### Query Parameters

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `query` | string | Main search text. Supports special syntax | - |
| `fuzzy` | boolean | Enable typo-tolerant search | true |
| `tags` | string[] | Filter by ALL tags (AND logic) | - |
| `created_by` | int[] | Filter by creator user IDs | - |
| `created_after` | datetime | Filter by creation date after | - |
| `created_before` | datetime | Filter by creation date before | - |
| `updated_after` | datetime | Filter by update date after | - |
| `updated_before` | datetime | Filter by update date before | - |
| `limit` | int | Results per page (1-100) | 20 |
| `offset` | int | Skip N results for pagination | 0 |
| `sort_by` | string | Sort field: relevance, name, created_at, updated_at | relevance |
| `sort_order` | string | Sort order: asc, desc | desc |
| `include_facets` | boolean | Include facet counts | true |
| `facet_fields` | string[] | Which facets to calculate | ['tags', 'created_by'] |

#### Special Query Syntax

The search supports special keywords within the query:
- `tag:finance` - Filter by specific tag
- `user:jsmith` or `by:jsmith` - Filter by creator
- Multiple keywords: `financial report tag:quarterly user:jsmith`

#### Example Request

```bash
curl -X GET "http://localhost:8000/api/datasets/search?query=financial%20report&tags=quarterly&tags=2024&limit=10" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

#### Example Response

```json
{
  "results": [
    {
      "id": 123,
      "name": "Q4 Financial Report 2024",
      "description": "Quarterly financial data for Q4 2024",
      "created_by": 456,
      "created_by_name": "jsmith",
      "created_at": "2024-01-15T10:30:00Z",
      "updated_at": "2024-01-20T14:45:00Z",
      "tags": ["finance", "quarterly", "2024"],
      "score": 0.85,
      "user_permission": "read"
    }
  ],
  "total": 42,
  "limit": 10,
  "offset": 0,
  "has_more": true,
  "query": "financial report",
  "execution_time_ms": 23,
  "facets": {
    "tags": {
      "finance": 25,
      "quarterly": 18,
      "2024": 15
    },
    "created_by": {
      "jsmith": 12,
      "adoe": 8
    }
  }
}
```

### 2. Autocomplete Suggestions

**Endpoint:** `GET /api/datasets/search/suggest`

Get autocomplete suggestions for dataset names and tags.

#### Query Parameters

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `query` | string | Partial text (required, min 1 char) | - |
| `limit` | int | Max suggestions (1-50) | 10 |

#### Example Request

```bash
curl -X GET "http://localhost:8000/api/datasets/search/suggest?query=finan&limit=5" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

#### Example Response

```json
{
  "suggestions": [
    {
      "text": "Financial Report Q4 2024",
      "type": "dataset_name",
      "score": 0.92
    },
    {
      "text": "finance",
      "type": "tag",
      "score": 0.88
    }
  ],
  "query": "finan",
  "execution_time_ms": 5
}
```

## Usage Examples

### Basic Search

```javascript
// Search for datasets containing "sales"
const response = await fetch('/api/datasets/search?query=sales');
const data = await response.json();
```

### Filtered Search

```javascript
// Search for financial datasets created in 2024
const params = new URLSearchParams({
  query: 'financial',
  tags: ['2024', 'quarterly'],
  sort_by: 'created_at',
  sort_order: 'desc'
});

const response = await fetch(`/api/datasets/search?${params}`);
```

### Using Special Syntax

```javascript
// Search using special keywords
const response = await fetch('/api/datasets/search?query=revenue%20tag:finance%20user:jsmith');
```

### Pagination

```javascript
// Get page 2 of results (items 21-40)
const response = await fetch('/api/datasets/search?query=data&limit=20&offset=20');
```

### Autocomplete

```javascript
// Get suggestions as user types
const response = await fetch('/api/datasets/search/suggest?query=fin');
const { suggestions } = await response.json();

// Display suggestions in dropdown
suggestions.forEach(suggestion => {
  console.log(`${suggestion.text} (${suggestion.type})`);
});
```

## Performance Considerations

1. **Fuzzy Search**: Enabled by default for better user experience but slightly slower
2. **Facets**: Calculate only needed facets to improve performance
3. **Pagination**: Use reasonable page sizes (10-50 items)
4. **Caching**: Results are not cached; consider client-side caching

## Database Maintenance

The search uses a materialized view that needs periodic refreshing:

```sql
-- Refresh the search index (run periodically or after bulk updates)
REFRESH MATERIALIZED VIEW CONCURRENTLY dsa_search.datasets_summary;
```

This can be automated with a cron job or triggered after dataset modifications.