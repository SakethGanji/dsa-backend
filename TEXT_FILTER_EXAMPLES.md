# Text-Based Filter Examples

## API Request Format

### Old Format (Array-based)
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

### New Format (Text-based)
```json
{
  "filters": {
    "expression": "age > 25 AND status = 'active'"
  }
}
```

## Example Filter Expressions

### Simple Filters
```
age > 25
status = 'active'
department != 'sales'
salary >= 50000
email LIKE '%@company.com'
created_at > '2024-01-01'
notes IS NULL
manager_id IS NOT NULL
```

### Combined Filters with AND/OR
```
age > 25 AND status = 'active'
department = 'sales' OR department = 'marketing'
salary > 50000 AND role != 'intern'
```

### Complex Filters with Parentheses
```
(age > 25 AND department = 'sales') OR (age > 30 AND department = 'marketing')
status = 'active' AND (role = 'admin' OR role = 'manager')
(price > 100 OR category = 'premium') AND stock > 0
((age >= 18 AND age <= 65) AND status = 'active') OR status = 'vip'
```

### IN/NOT IN Operators
```
department IN ('sales', 'marketing', 'engineering')
status NOT IN ('deleted', 'archived')
role IN ('admin', 'manager') AND department = 'IT'
```

### Pattern Matching
```
email LIKE '%@gmail.com'
name ILIKE 'john%'  -- case insensitive
description LIKE '%urgent%' AND status != 'completed'
```

## Full API Request Example

```json
{
  "source_ref": "main",
  "table_key": "primary",
  "output_branch_name": "sampled_active_sales",
  "commit_message": "Sample of active sales team members",
  "rounds": [
    {
      "round_number": 1,
      "method": "random",
      "parameters": {
        "sample_size": 1000,
        "seed": 42
      },
      "filters": {
        "expression": "(department = 'sales' OR department = 'business_dev') AND status = 'active' AND hire_date < '2024-01-01'"
      },
      "selection": {
        "columns": ["id", "name", "email", "department", "role"],
        "order_by": "hire_date",
        "order_desc": false
      }
    }
  ],
  "export_residual": false
}
```

## Benefits

1. **Readability**: Much easier to read and understand complex logic
2. **Flexibility**: Support for arbitrary nesting with parentheses
3. **Familiarity**: SQL-like syntax that most users understand
4. **Conciseness**: Single string instead of nested JSON structures
5. **Maintainability**: Easier to modify and debug filter logic