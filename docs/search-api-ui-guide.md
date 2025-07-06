# Search API UI Integration Guide

This guide provides everything the UI team needs to integrate with the DSA Search API.

## Quick Start

The Search API provides two main endpoints:
- **Search**: `GET /api/datasets/search` - Full-featured search with filters, pagination, and facets
- **Autocomplete**: `GET /api/datasets/search/suggest` - Fast suggestions for search-as-you-type

## Search Endpoint

### Basic Usage

```javascript
// Simple search
fetch('/api/datasets/search?query=financial%20report')

// With filters and pagination
fetch('/api/datasets/search?query=financial&tags=finance&tags=pii&limit=20&offset=0')

// Fuzzy search disabled (exact matching)
fetch('/api/datasets/search?query=financial&fuzzy=false')
```

### Request Parameters

All parameters are sent as URL query parameters.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query` | string | - | Main search text. Supports special syntax (see below) |
| `fuzzy` | boolean | `true` | Enable typo-tolerant search |
| `tags` | string[] | - | Filter by ALL specified tags (AND logic) |
| `created_by` | number[] | - | Filter by creator user IDs |
| `created_after` | ISO 8601 | - | Filter by creation date |
| `created_before` | ISO 8601 | - | Filter by creation date |
| `updated_after` | ISO 8601 | - | Filter by update date |
| `updated_before` | ISO 8601 | - | Filter by update date |
| `limit` | number | `20` | Results per page (1-100) |
| `offset` | number | `0` | Skip N results for pagination |
| `sort_by` | enum | `relevance` or `updated_at` | Sort field: `relevance`, `name`, `created_at`, `updated_at` |
| `sort_order` | enum | `desc` | Sort direction: `asc` or `desc` |
| `include_facets` | boolean | `true` | Include facet counts |
| `facet_fields` | string[] | `['tags', 'created_by']` | Which facets to calculate |

### Special Query Syntax

The `query` parameter supports special keywords:

```javascript
// Search for datasets with specific tag
"report tag:finance"

// Search for datasets by creator
"dashboard user:jsmith"
// or
"dashboard by:jsmith"

// Multi-word values need quotes
'analysis tag:"data quality"'

// Multiple filters
"quarterly tag:finance user:jsmith"
```

### Response Format

```typescript
interface SearchResponse {
  results: Array<{
    id: number;
    name: string;
    description?: string;
    created_by: number;         // User ID
    created_by_name: string;    // Username/SOEID for display
    created_at: string;         // ISO 8601
    updated_at: string;         // ISO 8601
    tags: string[];
    score: number;              // 0-1, only when sort_by='relevance'
    user_permission: 'read' | 'write' | 'admin';
  }>;
  
  // Pagination
  total: number;
  limit: number;
  offset: number;
  has_more: boolean;           // true if more pages exist
  
  // Metadata
  query?: string;              // Echo of search query
  execution_time_ms: number;
  
  // Facets (if requested)
  facets?: {
    tags?: { [tagName: string]: number };        // e.g. {"finance": 25, "pii": 10}
    created_by?: { [userId: string]: number };   // e.g. {"123": 15, "456": 8}
  };
}
```


## Autocomplete Endpoint

### Basic Usage

```javascript
// Get suggestions as user types
fetch('/api/datasets/search/suggest?query=fin&limit=5')
```

### Request Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query` | string | required | Partial text (min 1 character) |
| `limit` | number | `10` | Number of suggestions (1-50) |

### Response Format

```typescript
interface SuggestResponse {
  suggestions: Array<{
    text: string;              // "Financial Report" or "finance"
    type: 'dataset_name' | 'tag';
    score: number;             // 0-1 relevance
  }>;
  query: string;
  execution_time_ms: number;
}
```

### 1. Search with Filters Sidebar

```
┌─────────────────────────────────────────┐
│ [Search box with autocomplete]  [Search]│
├─────────────┬───────────────────────────┤
│ Filters     │ Results (showing 1-20 of 156)
│             │                           │
│ Tags        │ ┌─────────────────────┐ │
│ □ finance(25)│ │ Financial Report    │ │
│ ☑ pii (18)  │ │ 95% match          │ │
│ □ quarterly │ │ Created by jsmith   │ │
│             │ │ [finance][pii]      │ │
│ Created By  │ └─────────────────────┘ │
│ □ jsmith(12)│                         │
│ □ adoe (8)  │ ┌─────────────────────┐ │
│             │ │ Q4 Analysis        │ │
│ Date Range  │ │ 87% match          │ │
│ [Date picker]│ │ Created by adoe    │ │
│             │ │ [finance]          │ │
└─────────────┴───────────────────────────┘
```

### 2. Inline Tag Filters

```
Search: [financial report] 
Filtered by: [×finance] [×pii] [+Add filter]
Showing 42 results sorted by relevance ▼
```

### 3. Advanced Search Toggle



## Testing Checklist

- [ ] Search returns results based on query
- [ ] Fuzzy search handles typos (e.g., "finacial" → "financial")
- [ ] Special syntax works (tag:value, user:value)
- [ ] Filters combine correctly (AND logic for tags)
- [ ] Pagination works correctly
- [ ] Sort options work as expected
- [ ] Facets display and filter correctly
- [ ] Autocomplete suggestions appear quickly
- [ ] Empty states handled gracefully
- [ ] Error states show helpful messages
- [ ] Loading states prevent duplicate requests
- [ ] Results show correct permission levels
- [ ] Date filtering works with timezone handling
- [ ] URL parameters persist on page refresh
- [ ] Accessibility: keyboard navigation works
- [ ] Accessibility: screen reader announcements
- [ ] Performance: debounced autocomplete
- [ ] Performance: cached results where appropriate