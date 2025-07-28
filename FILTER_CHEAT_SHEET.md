# 🎯 Filter Language Cheat Sheet

## 🔵 Basic Syntax
```sql
column operator value
column operator value AND/OR column operator value
```

## 🟢 Quick Examples

### Numbers
```sql
age > 25                    ✓
salary >= 50000            ✓
price < 99.99              ✓
age > '25'                 ✗ (don't quote numbers)
```

### Strings
```sql
name = 'John'              ✓
status = 'active'          ✓
name = John                ✗ (must quote strings)
name = "John"              ✗ (use single quotes)
```

### Special Characters
```sql
name = 'O''Brien'          ✓ (double the quote)
name = 'O\'Brien'          ✗ (don't escape)
company = 'AT&T'           ✓ (& is fine)
```

## 🔴 Operators At-a-Glance

| Type | Operators | Example |
|------|-----------|---------|
| **Compare** | `= != <> > < >= <=` | `age > 25` |
| **List** | `IN` `NOT IN` | `role IN ('admin', 'user')` |
| **Pattern** | `LIKE` `ILIKE` `NOT LIKE` | `email LIKE '%@gmail%'` |
| **NULL** | `IS NULL` `IS NOT NULL` | `phone IS NULL` |
| **Logic** | `AND` `OR` `()` | `(A OR B) AND C` |

## 🟡 Pattern Wildcards

| Symbol | Meaning | Example | Matches |
|--------|---------|---------|---------|
| `%` | Any characters | `'J%'` | John, Jane, J, James |
| `_` | One character | `'J_n'` | Jon, Jan, Jen |
| `%%` | Contains | `'%admin%'` | administrator, admin, superadmin |

## 🟣 Common Patterns Copy/Paste

### Email Domains
```sql
-- Gmail users
email LIKE '%@gmail.com'

-- Corporate emails
email NOT LIKE '%@gmail.com' AND email NOT LIKE '%@yahoo.com'

-- Specific domain
email LIKE '%@company.com'
```

### Date Ranges
```sql
-- This year
created_at >= '2024-01-01'

-- Last 30 days (use current date - 30)
updated_at >= '2024-10-29'

-- Specific month
created_at >= '2024-01-01' AND created_at < '2024-02-01'
```

### Status Checks
```sql
-- Active records
status = 'active'

-- Not deleted
status NOT IN ('deleted', 'archived')

-- Multiple valid statuses
status IN ('active', 'pending', 'approved')
```

### Numeric Ranges
```sql
-- Age range
age >= 18 AND age <= 65

-- Price range
price > 10 AND price < 100

-- High values
amount >= 1000
```

### NULL Handling
```sql
-- Has no manager
manager_id IS NULL

-- Has email
email IS NOT NULL

-- Optional field is empty
notes IS NULL OR notes = ''
```

## 🔺 DO's and DON'Ts

### ✅ DO
```sql
status = 'active'                    -- Quote strings
age > 25                            -- Numbers without quotes
email IS NULL                       -- Use IS for NULL
name = 'O''Brien'                   -- Double single quotes
(A AND B) OR C                      -- Use parentheses for clarity
IN ('a', 'b', 'c')                  -- Use IN for multiple values
```

### ❌ DON'T
```sql
status = active                     -- Forgot quotes
age > '25'                         -- Don't quote numbers
email = NULL                       -- Wrong NULL syntax
name = 'O'Brien'                   -- Unescaped quote
A AND B OR C                       -- Ambiguous without ()
x = 'a' OR x = 'b' OR x = 'c'    -- Use IN instead
```

## 🎨 Real Examples

### E-commerce
```sql
-- High-value customers
total_purchases > 1000 AND status = 'active'

-- Recent orders
order_date >= '2024-01-01' AND status != 'cancelled'

-- Sale items
discount > 0 AND category IN ('electronics', 'clothing')
```

### HR/Employees
```sql
-- Senior staff
years_employed > 5 AND level >= 'senior'

-- Remote workers
location = 'remote' AND timezone IN ('EST', 'PST')

-- Review needed
last_review < '2023-07-01' AND status = 'active'
```

### SaaS/Users
```sql
-- Trial users
plan = 'trial' AND signup_date > '2024-01-01'

-- Engaged users
last_login > '2024-10-01' AND total_logins > 10

-- At risk
plan = 'premium' AND last_login < '2024-09-01'
```

## 🚀 Quick Formulas

| Need | Formula |
|------|---------|
| **Multiple choices** | `column IN ('option1', 'option2', 'option3')` |
| **Exclude values** | `column NOT IN ('bad1', 'bad2')` |
| **Text contains** | `column LIKE '%keyword%'` |
| **Text starts with** | `column LIKE 'prefix%'` |
| **Text ends with** | `column LIKE '%suffix'` |
| **Case insensitive** | `column ILIKE 'pattern'` |
| **Between range** | `column >= low AND column <= high` |
| **Empty check** | `column IS NULL OR column = ''` |
| **Has value** | `column IS NOT NULL AND column != ''` |

## 📋 Copy-Paste Templates

```sql
-- Basic filter
column = 'value'

-- Multiple conditions
column1 = 'value1' AND column2 > 123

-- Either/or
(column = 'value1' OR column = 'value2')

-- Complex
status = 'active' AND (role = 'admin' OR department = 'IT')

-- Range
created >= '2024-01-01' AND created < '2024-02-01'

-- List membership
category IN ('A', 'B', 'C')

-- Pattern search
description LIKE '%important%'

-- Null check
manager_id IS NOT NULL

-- Combination
age > 21 AND status = 'active' AND email LIKE '%@company.com'
```

## 🎯 Remember

1. **Strings** → Single quotes: `'value'`
2. **Numbers** → No quotes: `123`
3. **NULL** → Use IS/IS NOT: `IS NULL`
4. **Lists** → Use IN: `IN ('a','b')`
5. **Patterns** → Use %: `LIKE '%text%'`
6. **Groups** → Use (): `(A OR B) AND C`