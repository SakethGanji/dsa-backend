# Search API Documentation

## 1. Search API

### API Endpoint: `GET /api/datasets/search`

All parameters are sent as URL query string parameters.

### Request Interface (SearchRequest)

```typescript
interface SearchRequest {
  // === Core Search ===
  query?: string;             // The main text search query. Supports special syntax:
                             // - "tag:value" - filter by specific tag
                             // - "user:value" or "by:value" - filter by creator username
                             // - Use quotes for multi-word values: tag:"data quality"
  fuzzy?: boolean;           // Use trigram similarity for fuzzy/typo-tolerant search. (Default: true)

  // === Filters ===
  tags?: string[];           // Filter by datasets that contain ALL of these tags (AND logic).
  created_by?: number[];     // Filter by an array of creator user IDs.

  // Date filters (ISO 8601 format string: "YYYY-MM-DDTHH:mm:ssZ")
  created_after?: string;
  created_before?: string;
  updated_after?: string;
  updated_before?: string;

  // === Pagination ===
  limit?: number;            // Number of results to return. Range: 1-100. (Default: 20)
  offset?: number;           // Number of results to skip for pagination. (Default: 0)

  // === Sorting ===
  sort_by?: 'relevance' | 'name' | 'created_at' | 'updated_at'; // (Default: 'relevance' if query exists, otherwise 'updated_at')
  sort_order?: 'asc' | 'desc';                                  // (Default: 'desc')

  // === Facets ===
  include_facets?: boolean;  // Set to true to receive facet counts in the response. (Default: true)
  facet_fields?: ('tags' | 'created_by')[];                    // The specific facets to calculate. (Default: ['tags', 'created_by'])
}
```

### Response Interface (SearchResponse)

```typescript
interface SearchResponse {
  // The list of dataset results for the current page.
  results: SearchResult[];

  // Pagination metadata
  total: number;             // The total number of results matching the query.
  limit: number;             // The limit used for this request.
  offset: number;            // The offset used for this request.
  has_more: boolean;         // True if there are more results to fetch. (offset + limit < total)

  // Request metadata
  query?: string;            // The original query string from the request.
  execution_time_ms: number; // Time taken by the server to execute the search.

  // Facet data (only included if include_facets was true)
  facets?: SearchFacets;
}

interface SearchResult {
  id: number;                // The dataset ID.
  name: string;
  description?: string;
  created_by: number;        // The user ID of the creator.
  created_by_name: string;   // The username/SOEID of the creator.
  created_at: string;        // ISO 8601 timestamp.
  updated_at: string;        // ISO 8601 timestamp.
  tags: string[];
  score: number;             // Relevance score (0-1), only non-null when sorting by 'relevance'.
  user_permission: 'read' | 'write' | 'admin'; // The requesting user's permission level on the dataset.
}

// NOTE: The facets object is a simple key-value map. The UI is responsible for formatting this data for display.
interface SearchFacets {
  // Example: { "data-quality": 25, "finance": 18, "pii": 10 }
  tags?: { [tagName: string]: number };

  // Example: { "123": 42, "456": 15 } - Returns user IDs as keys
  created_by?: { [userId: string]: number };
}
```

### Example Request

```
GET /api/datasets/search?query=financial%20report&tags=finance&tags=pii&fuzzy=true&limit=20&offset=0&sort_by=relevance&include_facets=true
```

### Example Response

```json
{
  "results": [
    {
      "id": 123,
      "name": "Q4 Financial Report",
      "description": "Quarterly financial data",
      "created_by": 456,
      "created_by_name": "jsmith",
      "created_at": "2024-01-15T10:30:00Z",
      "updated_at": "2024-01-20T14:45:00Z",
      "tags": ["finance", "quarterly", "pii"],
      "score": 0.95,
      "user_permission": "read"
    }
  ],
  "total": 42,
  "limit": 20,
  "offset": 0,
  "has_more": true,
  "query": "financial report",
  "execution_time_ms": 15,
  "facets": {
    "tags": {
      "finance": 25,
      "pii": 18,
      "quarterly": 12
    },
    "created_by": {
      "456": 8,
      "789": 5
    }
  }
}
```

## 2. Suggest API (Autocomplete)

### API Endpoint: `GET /api/datasets/search/suggest`

### Request Interface (SuggestRequest)

```typescript
interface SuggestRequest {
  query: string;       // Required (min 1 character), the partial text to get suggestions for.
  limit?: number;      // Number of suggestions to return. Range: 1-50. (Default: 10)
}
```

### Response Interface (SuggestResponse)

```typescript
interface SuggestResponse {
  // The list of suggestions.
  suggestions: Suggestion[];

  // Request metadata
  query: string;
  execution_time_ms: number;
}

interface Suggestion {
  text: string;                            // The suggested text (e.g., "Financial Report" or "data-quality").
  type: 'dataset_name' | 'tag';           // The type of entity being suggested.
  score: number;                          // Relevance score (0-1), useful for ranking.
}
```

### Example Request

```
GET /api/datasets/search/suggest?query=fin&limit=5
```

### Example Response

```json
{
  "suggestions": [
    {
      "text": "Financial Report",
      "type": "dataset_name",
      "score": 0.9
    },
    {
      "text": "finance",
      "type": "tag",
      "score": 0.85
    }
  ],
  "query": "fin",
  "execution_time_ms": 5
}
```

## Key Implementation Details for UI

1. **Query Parameters**: All request parameters are sent via the URL query string.

2. **Array Parameters**: Arrays (`tags`, `created_by`, `facet_fields`) are sent as repeated query parameters.
   - Example: `...&tags=finance&tags=pii&created_by=123&created_by=456`

3. **Special Query Syntax**:
   - `tag:value` - Filters results to datasets with the specified tag
   - `user:username` or `by:username` - Filters results to datasets created by the specified user
   - Use quotes for multi-word values: `tag:"data quality"`
   - These keywords are extracted from the query and applied as filters

4. **Default Behavior**:
   - `fuzzy` defaults to `true`. This is the recommended mode for general user input.
   - `include_facets` defaults to `true`.
   - The default sort order is by `relevance` if a query string is present, otherwise by `updated_at` descending.

5. **UI Expectations**:
   - The UI will receive both `created_by` (user ID) and `created_by_name` (username/SOEID) in the search results.
   - All dates are ISO 8601 format strings and should be parsed by the UI for display.
   - The `score` is a float between 0 and 1. The UI can display this as a percentage or use it to render a relevance bar.
   - The `facets` object returns user IDs as keys (not usernames) for the `created_by` facet.
   - Results are filtered based on the requesting user's permissions - only datasets the user has access to are returned.

6. **Error Handling**:
   - Invalid `sort_by` or `sort_order` values return HTTP 400 with error details
   - Invalid date formats return HTTP 400
   - The API validates all parameters before processing

7. **Performance Notes**:
   - The search uses a materialized view for optimal performance
   - Fuzzy search uses PostgreSQL trigram similarity with a threshold of 0.2
   - Non-fuzzy search uses PostgreSQL full-text search
   - Facet calculations are optimized and limited to top results

8. **Authentication**:
   - The API requires authentication (specific header format depends on your auth setup)
   - Results are automatically filtered based on the authenticated user's permissions