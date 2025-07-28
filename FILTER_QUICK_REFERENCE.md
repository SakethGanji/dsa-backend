# Filter Expression Quick Reference

## Basic Syntax
```sql
column_name operator value
column_name operator value AND/OR column_name operator value
(expression) AND/OR (expression)
```

## Common Examples

### Numeric Comparisons
```sql
age > 25
salary >= 50000
price < 100.50
quantity != 0
```

### String Comparisons
```sql
status = 'active'
department != 'sales'
name = 'O''Brien'  -- Escaped quote
```

### List Operations
```sql
department IN ('sales', 'marketing', 'hr')
status NOT IN ('deleted', 'archived')
role IN ('admin', 'manager')
```

### Pattern Matching
```sql
email LIKE '%@gmail.com'        -- Ends with
name LIKE 'John%'               -- Starts with
description LIKE '%urgent%'      -- Contains
phone LIKE '555-___-____'       -- Pattern with wildcards

email NOT LIKE '%@test.com'     -- Doesn't match pattern
name ILIKE 'john%'              -- Case insensitive
```

### NULL Checks
```sql
manager_id IS NULL
notes IS NOT NULL
```

### Date Comparisons
```sql
created_at > '2024-01-01'
hire_date <= '2023-12-31'
last_login < '2024-01-01 15:30:00'
```

### Combined Conditions
```sql
-- Simple AND
age > 25 AND status = 'active'

-- Simple OR
department = 'sales' OR department = 'marketing'

-- Mixed with parentheses
status = 'active' AND (role = 'admin' OR role = 'manager')

-- Complex nested
(age > 25 AND department = 'sales') OR (age > 30 AND department = 'marketing')

-- Multiple conditions
age >= 18 AND age <= 65 AND status = 'active' AND department IN ('sales', 'marketing')
```

## Quick Tips

1. **Always quote strings**: `'value'` not `value`
2. **Escape quotes by doubling**: `'O''Brien'`
3. **Use parentheses to group**: `(A AND B) OR C`
4. **IN lists need parentheses**: `IN ('a', 'b', 'c')`
5. **NULL checks don't use equals**: `IS NULL` not `= NULL`
6. **Patterns are case-sensitive**: Use `ILIKE` for case-insensitive

## Full API Request Template

```bash
curl -X POST "http://localhost:8000/sampling/datasets/{dataset_id}/jobs" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "source_ref": "main",
    "table_key": "primary",
    "output_branch_name": "filtered_sample",
    "rounds": [{
      "round_number": 1,
      "method": "random",
      "parameters": {
        "sample_size": 1000,
        "filters": {
          "expression": "YOUR_FILTER_EXPRESSION_HERE"
        }
      }
    }]
  }'
```

## Common Patterns

### Active Users in Specific Departments
```sql
status = 'active' AND department IN ('sales', 'marketing', 'engineering')
```

### High-Value Customers
```sql
customer_type = 'premium' AND (total_purchases > 10000 OR account_age > 365)
```

### Recent Sign-ups from Gmail
```sql
email LIKE '%@gmail.com' AND created_at > '2024-01-01' AND verified = 'true'
```

### Employees Without Managers in Sales
```sql
department = 'sales' AND manager_id IS NULL AND employment_status = 'active'
```

### Complex Business Rule
```sql
((age >= 25 AND age <= 45) AND department IN ('sales', 'marketing')) 
OR (role = 'director' AND tenure_years > 5)
OR (is_vip = 'true')
```