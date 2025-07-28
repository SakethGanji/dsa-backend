# Filter Expression User Guide

## Quick Reference

### Basic Syntax

```
column = 'value'                    # Exact match
column > 100                        # Greater than
column < 100                        # Less than
column >= 100                       # Greater than or equal
column <= 100                       # Less than or equal
column != 'value'                   # Not equal

"column name" = 'value'             # Use double quotes for columns with spaces
```

### Combining Conditions

```
condition1 AND condition2           # Both must be true
condition1 OR condition2            # Either can be true
(condition1 OR condition2) AND condition3    # Use parentheses to group
```

## Common Examples

### Text Filtering

```
# Exact match
Make = 'TESLA'
status = 'active'

# Not equal
status != 'deleted'
Make <> 'UNKNOWN'                   # <> also means not equal

# Pattern matching
name LIKE 'John%'                   # Starts with John
email LIKE '%@gmail.com'            # Ends with @gmail.com
description LIKE '%important%'       # Contains important

# Case-insensitive pattern matching
name ILIKE 'john%'                  # Matches john, John, JOHN, etc.
```

### Number Filtering

```
age > 25
price <= 100.50
"Model Year" >= 2022                # Numbers as text need quotes
quantity != 0

# Note: If numbers are stored as text in your data, use quotes:
"Model Year" >= '2022'
"Electric Range" > '200'
```

### Multiple Values

```
# Check if value is in a list
status IN ('active', 'pending', 'approved')
Make IN ('TESLA', 'RIVIAN', 'LUCID')

# Check if value is NOT in a list
status NOT IN ('deleted', 'archived')
category NOT IN ('test', 'demo')
```

### NULL Checks

```
email IS NULL                       # Find records with no email
email IS NOT NULL                   # Find records with email
"Middle Name" IS NULL               # Columns with spaces need quotes
```

## Complex Filters

### Combining with AND

```
# All conditions must be true
age > 25 AND status = 'active'
Make = 'TESLA' AND "Model Year" >= '2022' AND County = 'King'
```

### Combining with OR

```
# Any condition can be true
status = 'active' OR status = 'pending'
Make = 'TESLA' OR Make = 'RIVIAN'
```

### Using Parentheses

```
# Group conditions for complex logic
(Make = 'TESLA' OR Make = 'RIVIAN') AND "Model Year" >= '2022'
(age > 25 AND age < 65) OR status = 'vip'
(city = 'Seattle' OR city = 'Bellevue') AND (status = 'active' OR priority = 'high')
```

## Real-World Examples

### Electric Vehicles

```
# Tesla vehicles from 2022 or newer
Make = 'TESLA' AND "Model Year" >= '2022'

# Electric vehicles with good range in King County
"Electric Range" > '200' AND County = 'King'

# Tesla or Rivian vehicles, 2022 or newer, with 200+ mile range
(Make = 'TESLA' OR Make = 'RIVIAN') AND "Model Year" >= '2022' AND "Electric Range" > '200'
```

### Customer Data

```
# Active customers in Seattle
status = 'active' AND city = 'Seattle'

# High-value customers
total_purchases > 1000 AND customer_tier = 'gold'

# Recent signups
created_date >= '2024-01-01' AND email IS NOT NULL
```

### Product Catalog

```
# In-stock products under $50
price < 50 AND inventory > 0

# Electronics or Appliances on sale
(category = 'Electronics' OR category = 'Appliances') AND discount > 0

# Products needing restock
inventory < 10 AND status = 'active' AND discontinued != 'true'
```

## Important Rules

### 1. Quotes Usage

- **Double quotes** `"` for column names with spaces: `"Model Year"`, `"First Name"`
- **Single quotes** `'` for text values: `'TESLA'`, `'active'`, `'2024-01-01'`
- Numbers can be quoted or unquoted, depending on your data

### 2. Case Sensitivity

- Keywords (`AND`, `OR`, `IN`, `LIKE`) are not case-sensitive
- Column names preserve their case
- Text values are case-sensitive unless using `ILIKE`

### 3. Special Characters in Values

```
# If your value contains single quotes, escape them
name = 'O''Brien'                   # For O'Brien

# Percent signs in LIKE patterns
description LIKE '%50\%%'           # Contains "50%"
```

### 4. Common Mistakes to Avoid

```
# ❌ WRONG - Missing quotes on text value
status = active

# ✅ CORRECT
status = 'active'

# ❌ WRONG - Using single quotes for column with spaces
'Model Year' = 2022

# ✅ CORRECT - Use double quotes for columns
"Model Year" = '2022'

# ❌ WRONG - Empty IN list
status IN ()

# ✅ CORRECT - IN list needs values
status IN ('active', 'pending')
```

## Pattern Matching with LIKE

### Wildcards

- `%` matches any sequence of characters
- `_` matches exactly one character

### Examples

```
# Starts with
name LIKE 'John%'                   # John, Johnny, Johnson

# Ends with  
email LIKE '%@company.com'          # any@company.com

# Contains
description LIKE '%urgent%'         # Contains "urgent" anywhere

# Specific pattern
phone LIKE '206-___-____'          # 206-XXX-XXXX format

# Multiple wildcards
file_name LIKE '%.pdf'             # Any PDF file
code LIKE 'ABC_%_%'                # ABC followed by at least 2 characters
```

## Tips for Success

1. **Start Simple**: Begin with one condition, then add more
2. **Test Incrementally**: Add conditions one at a time to debug
3. **Use Parentheses**: When in doubt, add parentheses to clarify logic
4. **Check Your Quotes**: Double for columns with spaces, single for values
5. **Know Your Data**: Check if numbers are stored as text in your dataset

## Quick Troubleshooting

| Error | Solution |
|-------|----------|
| "Invalid column" | Check spelling, add double quotes if column has spaces |
| "Invalid character" | Make sure to use straight quotes ' " not curly quotes ' " |
| "Expected value" | Add quotes around text values |
| "Empty IN list" | Add at least one value to IN(...) |
| "0 results" | Check if values are stored as text (add quotes to numbers) |

## Need More Help?

- Try simpler filters first to verify column names
- Check data types - numbers might be stored as text
- Use parentheses to make complex logic clearer
- Test each part of your filter separately