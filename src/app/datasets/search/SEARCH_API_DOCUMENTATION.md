# Unified Dataset Search API Documentation

## Overview

The Unified Dataset Search API provides powerful search capabilities for discovering datasets within the Data Science API platform. It supports full-text search, fuzzy matching, advanced filtering, faceted search, and autocomplete suggestions.

## Features

- **Full-Text Search**: Search across dataset names and descriptions
- **Fuzzy Search**: Typo-tolerant search using PostgreSQL's pg_trgm extension
- **Advanced Filtering**: Filter by tags, file types, dates, file sizes, and more
- **Faceted Search**: Get aggregated counts for common filter values
- **Autocomplete**: Real-time search suggestions as you type
- **Permission-Aware**: Only returns datasets the user has access to
- **Relevance Ranking**: Results sorted by relevance when searching

## API Endpoints

### 1. Search Datasets (POST)

**Endpoint**: `POST /api/datasets/search`

**Description**: Perform advanced dataset search with full filtering capabilities.

**Request Body**:
```json
{
  "query": "machine learning",
  "tags": ["ml", "classification"],
  "file_types": ["parquet", "csv"],
  "created_by": [123, 456],
  "created_at": {
    "start": "2024-01-01T00:00:00Z",
    "end": "2024-12-31T23:59:59Z"
  },
  "file_size": {
    "min": 1000000,
    "max": 100000000
  },
  "fuzzy_search": true,
  "search_in_description": true,
  "search_in_tags": true,
  "limit": 20,
  "offset": 0,
  "sort_by": "relevance",
  "sort_order": "desc",
  "include_facets": true
}
```

**Response**:
```json
{
  "results": [
    {
      "id": 123,
      "name": "Machine Learning Dataset",
      "description": "Dataset for ML experiments",
      "created_by": 456,
      "created_by_name": "john_doe",
      "created_at": "2024-03-15T10:30:00Z",
      "updated_at": "2024-03-20T14:45:00Z",
      "current_version": 3,
      "version_count": 3,
      "file_type": "parquet",
      "file_size": 25000000,
      "tags": ["ml", "classification", "supervised"],
      "score": 0.95,
      "user_permission": "read"
    }
  ],
  "total": 150,
  "limit": 20,
  "offset": 0,
  "has_more": true,
  "query": "machine learning",
  "execution_time_ms": 45.3,
  "facets": {
    "tags": {
      "field": "tags",
      "label": "Tags",
      "values": [
        {"value": "ml", "count": 45},
        {"value": "classification", "count": 23}
      ],
      "total_values": 15
    },
    "file_types": {
      "field": "file_types",
      "label": "File Types",
      "values": [
        {"value": "parquet", "count": 89},
        {"value": "csv", "count": 61}
      ],
      "total_values": 5
    }
  }
}
```

### 2. Search Datasets (GET)

**Endpoint**: `GET /api/datasets/search`

**Description**: Simple search using query parameters.

**Query Parameters**:
- `query`: Search query string
- `tags`: Comma-separated list of tags
- `file_types`: Comma-separated list of file types
- `fuzzy`: Enable fuzzy search (true/false)
- `limit`: Results per page (1-100)
- `offset`: Number of results to skip
- `sort_by`: Sort field (relevance, name, created_at, updated_at, file_size)
- `sort_order`: Sort order (asc/desc)

**Example**:
```
GET /api/datasets/search?query=sales&tags=finance,quarterly&limit=10&fuzzy=true
```

### 3. Search Suggestions

**Endpoint**: `POST /api/datasets/search/suggest`

**Description**: Get autocomplete suggestions for search queries.

**Request Body**:
```json
{
  "query": "mach",
  "limit": 10,
  "types": ["dataset_name", "tag"]
}
```

**Response**:
```json
{
  "suggestions": [
    {
      "text": "Machine Learning Dataset",
      "type": "dataset_name",
      "score": 0.89
    },
    {
      "text": "machine-learning",
      "type": "tag",
      "score": 0.85
    }
  ],
  "query": "mach",
  "execution_time_ms": 5.2
}
```

### 4. Initialize Search

**Endpoint**: `POST /api/datasets/search/init`

**Description**: Initialize database extensions and indexes for search (admin only).

**Response**:
```json
{
  "status": "success",
  "message": "Search capabilities initialized successfully"
}
```

## Search Query Syntax

### Basic Search
- Simple text search: `machine learning`
- Searches in dataset names and descriptions by default

### Fuzzy Search
- Enable with `fuzzy_search: true`
- Tolerates typos and spelling variations
- Example: `machne lerning` will match "machine learning"

### Filters

#### Tag Filters
- Exact match: `tags: ["finance", "quarterly"]`
- Datasets must have ALL specified tags

#### Date Range Filters
```json
{
  "created_at": {
    "start": "2024-01-01T00:00:00Z",
    "end": "2024-12-31T23:59:59Z"
  }
}
```

#### Numeric Range Filters
```json
{
  "file_size": {
    "min": 1000000,  // 1MB
    "max": 100000000 // 100MB
  }
}
```

## Sorting Options

- `relevance`: Sort by search relevance (default when query provided)
- `name`: Sort alphabetically by dataset name
- `created_at`: Sort by creation date
- `updated_at`: Sort by last update date
- `file_size`: Sort by file size
- `version_count`: Sort by number of versions

## Faceted Search

Facets provide aggregated counts for filter values. Available facets:
- `tags`: Most common tags with counts
- `file_types`: File type distribution
- `created_by`: Top dataset creators
- `years`: Dataset creation years

## Permissions

- Search results are automatically filtered based on user permissions
- Only datasets where the user has at least `read` permission are returned
- The `user_permission` field indicates the user's permission level for each result

## Performance Considerations

1. **Indexing**: Full-text and trigram indexes are used for fast search
2. **Caching**: Search results can be cached for repeated queries
3. **Pagination**: Use appropriate `limit` values to avoid large result sets
4. **Facets**: Disable facets (`include_facets: false`) for faster response if not needed

## Examples

### 1. Search for ML datasets created in 2024
```json
{
  "query": "machine learning",
  "created_at": {
    "start": "2024-01-01T00:00:00Z",
    "end": "2024-12-31T23:59:59Z"
  },
  "tags": ["ml"],
  "sort_by": "created_at",
  "sort_order": "desc"
}
```

### 2. Find large Parquet files with fuzzy search
```json
{
  "query": "sales data",
  "fuzzy_search": true,
  "file_types": ["parquet"],
  "file_size": {
    "min": 50000000  // 50MB
  }
}
```

### 3. Get all datasets with specific tags
```json
{
  "tags": ["finance", "quarterly", "2024"],
  "sort_by": "updated_at",
  "sort_order": "desc",
  "include_facets": false
}
```

## Error Handling

Common error responses:

- `400 Bad Request`: Invalid search parameters
- `401 Unauthorized`: Missing or invalid authentication
- `403 Forbidden`: Insufficient permissions
- `500 Internal Server Error`: Server-side error during search

## Migration Guide

To migrate from the old list endpoint to the new search API:

1. Replace `GET /api/datasets?name=...` with `GET /api/datasets/search?query=...`
2. The new API returns more detailed results with relevance scores
3. Facets provide additional filtering insights
4. Fuzzy search can improve user experience

## Future Enhancements

Planned features for future releases:

1. **Search Within Files**: Search dataset contents, not just metadata
2. **Advanced Query Language**: Support for boolean operators (AND, OR, NOT)
3. **Saved Searches**: Save and share common searches
4. **Search Analytics**: Track popular searches and improve relevance
5. **ML-Based Ranking**: Use machine learning to improve result ranking