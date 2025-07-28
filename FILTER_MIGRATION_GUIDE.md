# Filter Migration Guide: Array-based to Text-based

## Overview

This guide helps you migrate from the old array-based filter format to the new text-based expression format. The new format is more readable, flexible, and easier to maintain.

## Key Benefits of Migration

1. **Readability**: SQL-like syntax that developers already know
2. **Flexibility**: Easy to add complex conditions without nested JSON
3. **Maintainability**: Simpler to modify and debug
4. **Expressiveness**: Support for parentheses and complex logic

## Migration Examples

### Simple Equality

**Old Format:**
```json
{
  "filters": {
    "conditions": [
      {"column": "status", "operator": "=", "value": "active"}
    ],
    "logic": "AND"
  }
}
```

**New Format:**
```json
{
  "filters": {
    "expression": "status = 'active'"
  }
}
```

### Multiple Conditions with AND

**Old Format:**
```json
{
  "filters": {
    "conditions": [
      {"column": "age", "operator": ">", "value": 25},
      {"column": "status", "operator": "=", "value": "active"},
      {"column": "department", "operator": "!=", "value": "temp"}
    ],
    "logic": "AND"
  }
}
```

**New Format:**
```json
{
  "filters": {
    "expression": "age > 25 AND status = 'active' AND department != 'temp'"
  }
}
```

### OR Logic

**Old Format:**
```json
{
  "filters": {
    "conditions": [
      {"column": "department", "operator": "=", "value": "sales"},
      {"column": "department", "operator": "=", "value": "marketing"}
    ],
    "logic": "OR"
  }
}
```

**New Format:**
```json
{
  "filters": {
    "expression": "department = 'sales' OR department = 'marketing'"
  }
}
```

Or better with IN:
```json
{
  "filters": {
    "expression": "department IN ('sales', 'marketing')"
  }
}
```

### Complex Nested Logic

**Old Format (Limited):**
```json
{
  "filters": {
    "conditions": [
      // Old format couldn't easily express: 
      // (age > 25 AND dept = 'sales') OR (age > 30 AND dept = 'marketing')
    ],
    "logic": "AND"
  }
}
```

**New Format:**
```json
{
  "filters": {
    "expression": "(age > 25 AND department = 'sales') OR (age > 30 AND department = 'marketing')"
  }
}
```

### IN Operator

**Old Format:**
```json
{
  "filters": {
    "conditions": [
      {"column": "role", "operator": "in", "value": ["admin", "manager", "director"]}
    ],
    "logic": "AND"
  }
}
```

**New Format:**
```json
{
  "filters": {
    "expression": "role IN ('admin', 'manager', 'director')"
  }
}
```

### NULL Checks

**Old Format:**
```json
{
  "filters": {
    "conditions": [
      {"column": "manager_id", "operator": "is_null", "value": null}
    ],
    "logic": "AND"
  }
}
```

**New Format:**
```json
{
  "filters": {
    "expression": "manager_id IS NULL"
  }
}
```

### Pattern Matching

**Old Format:**
```json
{
  "filters": {
    "conditions": [
      {"column": "email", "operator": "like", "value": "%@gmail.com"}
    ],
    "logic": "AND"
  }
}
```

**New Format:**
```json
{
  "filters": {
    "expression": "email LIKE '%@gmail.com'"
  }
}
```

## Conversion Patterns

### Operator Mapping

| Old Operator | New Operator | Example |
|--------------|--------------|---------|
| `=` | `=` | `status = 'active'` |
| `!=` | `!=` or `<>` | `status != 'deleted'` |
| `>` | `>` | `age > 25` |
| `<` | `<` | `price < 100` |
| `>=` | `>=` | `salary >= 50000` |
| `<=` | `<=` | `quantity <= 10` |
| `in` | `IN` | `dept IN ('a', 'b')` |
| `not_in` | `NOT IN` | `status NOT IN ('x', 'y')` |
| `like` | `LIKE` | `email LIKE '%@%'` |
| `ilike` | `ILIKE` | `name ILIKE 'john%'` |
| `is_null` | `IS NULL` | `notes IS NULL` |
| `is_not_null` | `IS NOT NULL` | `manager_id IS NOT NULL` |

### Value Formatting

1. **Strings**: Always quote with single quotes
   - Old: `{"value": "active"}`
   - New: `'active'`

2. **Numbers**: Use directly without quotes
   - Old: `{"value": 25}`
   - New: `25`

3. **Lists**: Use parentheses with comma separation
   - Old: `{"value": ["a", "b", "c"]}`
   - New: `('a', 'b', 'c')`

4. **Special Characters**: Escape single quotes by doubling
   - Old: `{"value": "O'Brien"}`
   - New: `'O''Brien'`

## Step-by-Step Migration Process

### 1. Identify Current Filter Usage
```python
# Find all places using old format
old_filter = {
    "filters": {
        "conditions": [...],
        "logic": "AND"
    }
}
```

### 2. Convert to Expression
```python
# Convert each condition
conditions = []
for condition in old_filter["filters"]["conditions"]:
    col = condition["column"]
    op = condition["operator"]
    val = condition["value"]
    
    # Format based on operator
    if op == "is_null":
        conditions.append(f"{col} IS NULL")
    elif op == "is_not_null":
        conditions.append(f"{col} IS NOT NULL")
    elif op in ["in", "not_in"]:
        values = ", ".join([f"'{v}'" for v in val])
        op_str = "IN" if op == "in" else "NOT IN"
        conditions.append(f"{col} {op_str} ({values})")
    elif isinstance(val, str):
        conditions.append(f"{col} {op} '{val}'")
    else:
        conditions.append(f"{col} {op} {val}")

# Join with logic operator
logic = old_filter["filters"]["logic"]
expression = f" {logic} ".join(conditions)

# New format
new_filter = {
    "filters": {
        "expression": expression
    }
}
```

### 3. Test the Migration
```python
# Test with sample data
test_cases = [
    # Simple case
    {
        "old": {
            "conditions": [{"column": "age", "operator": ">", "value": 25}],
            "logic": "AND"
        },
        "expected": "age > 25"
    },
    # Complex case
    {
        "old": {
            "conditions": [
                {"column": "status", "operator": "=", "value": "active"},
                {"column": "role", "operator": "in", "value": ["admin", "user"]}
            ],
            "logic": "AND"
        },
        "expected": "status = 'active' AND role IN ('admin', 'user')"
    }
]
```

## Common Pitfalls

1. **Forgetting to Quote Strings**
   - Wrong: `status = active`
   - Right: `status = 'active'`

2. **Not Escaping Single Quotes**
   - Wrong: `name = 'O'Brien'`
   - Right: `name = 'O''Brien'`

3. **Using = with NULL**
   - Wrong: `notes = NULL`
   - Right: `notes IS NULL`

4. **Invalid Parentheses in IN**
   - Wrong: `dept IN 'sales', 'marketing'`
   - Right: `dept IN ('sales', 'marketing')`

## Backward Compatibility

The API maintains full backward compatibility. You can:
1. Continue using the old format (not recommended)
2. Migrate gradually, endpoint by endpoint
3. Run both formats in parallel during transition

## Support and Resources

- [API Filter Guide](./API_FILTER_GUIDE.md) - Complete API documentation
- [Quick Reference](./FILTER_QUICK_REFERENCE.md) - Quick syntax reference
- [Examples](./examples/filter_api_client.py) - Python client examples

## Migration Checklist

- [ ] Identify all API calls using old filter format
- [ ] Create mapping of your common filter patterns
- [ ] Update client code to generate new format
- [ ] Test with sample data
- [ ] Deploy with monitoring
- [ ] Remove old format support from client code
- [ ] Document any custom patterns for your team