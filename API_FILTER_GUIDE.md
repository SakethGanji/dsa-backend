# Text-Based Filter API Guide

## Overview

The sampling API now supports SQL-like text-based filter expressions, providing a more intuitive and flexible way to filter data during sampling operations. This guide covers the API changes, request/response formats, and comprehensive examples.

## API Endpoint

### Create Sampling Job
```
POST /sampling/datasets/{dataset_id}/jobs
```

## Filter Format Changes

### Old Format (Still Supported - Array-based)
```json
{
  "filters": {
    "conditions": [
      {"column": "age", "operator": ">", "value": 25},
      {"column": "status", "operator": "=", "value": "active"}
    ],
    "logic": "AND"
  }
}
```

### New Format (Text-based Expression)
```json
{
  "filters": {
    "expression": "age > 25 AND status = 'active'"
  }
}
```

## Request Schema

### Complete Request Structure
```json
{
  "source_ref": "string",           // Source branch/ref (default: "main")
  "table_key": "string",            // Table to sample (default: "primary")
  "output_branch_name": "string",   // Optional: Name for output branch
  "commit_message": "string",       // Optional: Commit message
  "rounds": [                       // Array of sampling rounds
    {
      "round_number": 1,
      "method": "string",           // "random", "stratified", "cluster", "systematic"
      "parameters": {
        // Method-specific parameters
        "sample_size": 1000,        // For random/stratified
        "seed": 42,                 // Optional: For reproducibility
        "filters": {                // NEW: Filter specification
          "expression": "string"    // SQL-like filter expression
        },
        "selection": {              // Optional: Column selection
          "columns": ["col1", "col2"],
          "order_by": "col_name",
          "order_desc": false
        }
      },
      "output_name": "string"       // Optional: Name for this round
    }
  ],
  "export_residual": false,         // Export unsampled records
  "residual_output_name": "string"  // Optional: Name for residual
}
```

## Filter Expression Syntax

### Supported Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `>` | Greater than | `age > 25` |
| `<` | Less than | `price < 100` |
| `>=` | Greater than or equal | `salary >= 50000` |
| `<=` | Less than or equal | `quantity <= 10` |
| `=` | Equal | `status = 'active'` |
| `!=` or `<>` | Not equal | `department != 'sales'` |
| `IN` | In list | `role IN ('admin', 'manager')` |
| `NOT IN` | Not in list | `status NOT IN ('deleted', 'archived')` |
| `LIKE` | Pattern match | `email LIKE '%@gmail.com'` |
| `ILIKE` | Case-insensitive pattern | `name ILIKE 'john%'` |
| `NOT LIKE` | Negative pattern match | `email NOT LIKE '%@test.com'` |
| `NOT ILIKE` | Case-insensitive negative | `name NOT ILIKE 'test%'` |
| `IS NULL` | Null check | `notes IS NULL` |
| `IS NOT NULL` | Not null check | `manager_id IS NOT NULL` |
| `AND` | Logical AND | `age > 25 AND status = 'active'` |
| `OR` | Logical OR | `dept = 'sales' OR dept = 'marketing'` |

### Pattern Matching
- `%` - Matches any sequence of characters
- `_` - Matches any single character (standard SQL)

## Complete API Examples

### Example 1: Simple Random Sampling with Filter
```bash
curl -X POST "http://localhost:8000/sampling/datasets/1/jobs" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "source_ref": "main",
    "table_key": "primary",
    "output_branch_name": "active_users_sample",
    "commit_message": "Sample of active users over 25",
    "rounds": [{
      "round_number": 1,
      "method": "random",
      "parameters": {
        "sample_size": 1000,
        "seed": 42,
        "filters": {
          "expression": "age > 25 AND status = '\''active'\''"
        }
      }
    }]
  }'
```

**Response:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "message": "Sampling job created successfully"
}
```

### Example 2: Complex Filter with Multiple Conditions
```bash
curl -X POST "http://localhost:8000/sampling/datasets/1/jobs" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "source_ref": "main",
    "table_key": "primary",
    "output_branch_name": "sales_team_sample",
    "rounds": [{
      "round_number": 1,
      "method": "random",
      "parameters": {
        "sample_size": 500,
        "filters": {
          "expression": "(department = '\''sales'\'' OR department = '\''business_dev'\'') AND status = '\''active'\'' AND hire_date < '\''2024-01-01'\'' AND salary > 50000"
        }
      }
    }]
  }'
```

### Example 3: Stratified Sampling with Filters
```bash
curl -X POST "http://localhost:8000/sampling/datasets/1/jobs" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "source_ref": "main",
    "table_key": "primary",
    "output_branch_name": "stratified_active_by_dept",
    "rounds": [{
      "round_number": 1,
      "method": "stratified",
      "parameters": {
        "strata_columns": ["department"],
        "sample_size": 1000,
        "filters": {
          "expression": "status = '\''active'\'' AND manager_id IS NOT NULL"
        }
      }
    }]
  }'
```

### Example 4: Multi-Round Sampling with Different Filters
```bash
curl -X POST "http://localhost:8000/sampling/datasets/1/jobs" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "source_ref": "main",
    "table_key": "primary",
    "output_branch_name": "multi_segment_sample",
    "rounds": [
      {
        "round_number": 1,
        "method": "random",
        "parameters": {
          "sample_size": 200,
          "filters": {
            "expression": "department = '\''engineering'\'' AND role IN ('\''senior'\', '\''lead'\'')"
          }
        },
        "output_name": "Senior Engineers"
      },
      {
        "round_number": 2,
        "method": "random",
        "parameters": {
          "sample_size": 300,
          "filters": {
            "expression": "department = '\''sales'\'' AND region = '\''NA'\'' AND quota_met = '\''true'\''"
          }
        },
        "output_name": "Top NA Sales"
      }
    ]
  }'
```

### Example 5: Pattern Matching and NULL Checks
```bash
curl -X POST "http://localhost:8000/sampling/datasets/1/jobs" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "source_ref": "main",
    "table_key": "primary",
    "output_branch_name": "gmail_users_with_managers",
    "rounds": [{
      "round_number": 1,
      "method": "random",
      "parameters": {
        "sample_size": 500,
        "filters": {
          "expression": "email LIKE '\''%@gmail.com'\'' AND manager_id IS NOT NULL AND notes NOT LIKE '\''%temp%'\''"
        }
      }
    }]
  }'
```

### Example 6: Using IN/NOT IN Operators
```bash
curl -X POST "http://localhost:8000/sampling/datasets/1/jobs" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "source_ref": "main",
    "table_key": "primary",
    "output_branch_name": "specific_departments",
    "rounds": [{
      "round_number": 1,
      "method": "random",
      "parameters": {
        "sample_size": 1000,
        "filters": {
          "expression": "department IN ('\''sales'\'', '\''marketing'\'', '\''engineering'\'') AND status NOT IN ('\''deleted'\'', '\''archived'\'', '\''suspended'\'')"
        }
      }
    }]
  }'
```

### Example 7: Complex Nested Conditions
```bash
curl -X POST "http://localhost:8000/sampling/datasets/1/jobs" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "source_ref": "main",
    "table_key": "primary",
    "output_branch_name": "complex_segment",
    "rounds": [{
      "round_number": 1,
      "method": "random",
      "parameters": {
        "sample_size": 750,
        "filters": {
          "expression": "((age >= 25 AND age <= 45) AND (department = '\''sales'\'' OR department = '\''marketing'\'')) OR (role = '\''director'\'' AND tenure_years > 5)"
        }
      }
    }]
  }'
```

## Error Responses

### Invalid Column Name
```json
{
  "detail": "Invalid column: unknown_column",
  "status_code": 400
}
```

### Syntax Error in Expression
```json
{
  "detail": "Expected operator at position 15",
  "status_code": 400
}
```

### Empty IN List
```json
{
  "detail": "IN list cannot be empty",
  "status_code": 400
}
```

### Unmatched Parentheses
```json
{
  "detail": "Expected ')' at position 42",
  "status_code": 400
}
```

## Migration Guide

### For Existing Integrations
The API maintains **backward compatibility**. Existing array-based filter formats will continue to work. However, we recommend migrating to the new text-based format for better readability and flexibility.

### Migration Example

**Old Format:**
```json
{
  "filters": {
    "conditions": [
      {"column": "age", "operator": ">", "value": 25},
      {"column": "status", "operator": "=", "value": "active"},
      {"column": "department", "operator": "in", "value": ["sales", "marketing"]}
    ],
    "logic": "AND"
  }
}
```

**New Format:**
```json
{
  "filters": {
    "expression": "age > 25 AND status = 'active' AND department IN ('sales', 'marketing')"
  }
}
```

## Best Practices

1. **Quote String Values**: Always use single quotes for string values
   ```
   status = 'active'  ✓
   status = active    ✗
   ```

2. **Escape Single Quotes**: Double single quotes to escape
   ```
   name = 'O''Brien'  ✓
   ```

3. **Use Parentheses for Clarity**: Group complex conditions
   ```
   (age > 25 AND dept = 'sales') OR (age > 30 AND dept = 'marketing')
   ```

4. **Column Name Validation**: Only use columns that exist in your schema
   - Valid columns are validated against the table schema
   - Invalid columns will result in a 400 error

5. **Type Safety**: The system automatically applies appropriate type casting
   - Numeric columns: Compared as numbers
   - Date/timestamp columns: Compared as dates
   - Text columns: Compared as strings

## Performance Considerations

1. **Indexed Columns**: Filters on indexed columns perform better
2. **Complex Expressions**: Deeply nested expressions may impact performance
3. **Pattern Matching**: LIKE with leading `%` (e.g., `%@gmail.com`) may be slower than prefix matches

## Security

- All filter expressions are parsed and validated before execution
- SQL injection is prevented through parameterized queries
- Column names are validated against the schema whitelist
- No raw SQL execution is allowed

## Limitations

1. **Max Expression Length**: 1000 characters
2. **Max Nesting Depth**: 10 levels of parentheses
3. **Supported Functions**: Currently no SQL functions (e.g., UPPER, LOWER) are supported
4. **Boolean Literals**: Use quoted strings ('true', 'false') for boolean comparisons

