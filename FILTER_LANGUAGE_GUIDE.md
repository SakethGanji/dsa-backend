# Filter Expression Language Guide

## 🚀 Quick Start

The filter expression language is a SQL-like syntax for filtering data. If you know SQL WHERE clauses, you already know 90% of this language!

```sql
age > 25 AND status = 'active'
```

## 📖 Language Basics

### 1. Basic Structure
```
column_name operator value
```

### 2. Combining Conditions
```
condition1 AND condition2
condition1 OR condition2
(condition1 AND condition2) OR condition3
```

## 🔤 Data Types

### Strings
Always use **single quotes** for string values:
```sql
name = 'John'
status = 'active'
city = 'Seattle'
```

To include a single quote in a string, **double it**:
```sql
name = 'O''Brien'  -- Represents: O'Brien
company = 'Bob''s Burgers'  -- Represents: Bob's Burgers
```

### Numbers
Use numbers directly without quotes:
```sql
age > 25
salary >= 50000
price < 99.99
quantity = 0
```

### Dates and Timestamps
Treat as strings in ISO format:
```sql
created_at > '2024-01-01'
birth_date <= '1990-12-31'
last_login < '2024-01-01 15:30:00'
```

### Booleans
Use quoted strings:
```sql
is_active = 'true'
is_verified = 'false'
```

### NULL Values
Special syntax for NULL:
```sql
manager_id IS NULL
notes IS NOT NULL
```

## 🔧 Operators

### Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equal to | `status = 'active'` |
| `!=` | Not equal to | `status != 'deleted'` |
| `<>` | Not equal to (alternative) | `status <> 'deleted'` |
| `>` | Greater than | `age > 18` |
| `<` | Less than | `price < 100` |
| `>=` | Greater than or equal | `salary >= 50000` |
| `<=` | Less than or equal | `stock <= 10` |

### List Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `IN` | Value is in list | `color IN ('red', 'blue', 'green')` |
| `NOT IN` | Value is not in list | `status NOT IN ('deleted', 'archived')` |

### Pattern Matching Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `LIKE` | Pattern match (case-sensitive) | `email LIKE '%@gmail.com'` |
| `ILIKE` | Pattern match (case-insensitive) | `name ILIKE 'john%'` |
| `NOT LIKE` | Negative pattern match | `email NOT LIKE '%@test.com'` |
| `NOT ILIKE` | Negative case-insensitive match | `name NOT ILIKE 'admin%'` |

#### Pattern Wildcards:
- `%` - Matches any sequence of characters (including none)
- `_` - Matches exactly one character

### NULL Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `IS NULL` | Value is NULL | `phone IS NULL` |
| `IS NOT NULL` | Value is not NULL | `email IS NOT NULL` |

### Logical Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `AND` | Both conditions must be true | `age > 25 AND status = 'active'` |
| `OR` | At least one condition must be true | `dept = 'sales' OR dept = 'marketing'` |
| `()` | Group conditions | `(age > 25 OR vip = 'true') AND active = 'true'` |

## 📝 Pattern Matching Examples

### LIKE Patterns

```sql
-- Ends with @gmail.com
email LIKE '%@gmail.com'

-- Starts with John
name LIKE 'John%'

-- Contains 'admin' anywhere
role LIKE '%admin%'

-- Exactly 5 characters starting with A
code LIKE 'A____'

-- Phone pattern (like 555-123-4567)
phone LIKE '___-___-____'

-- Ends with .pdf
filename LIKE '%.pdf'
```

### Case-Insensitive with ILIKE

```sql
-- Matches john, John, JOHN, JoHn, etc.
name ILIKE 'john%'

-- Contains urgent in any case
description ILIKE '%urgent%'
```

## 🎯 Common Patterns

### Range Checks
```sql
-- Age between 25 and 65
age >= 25 AND age <= 65

-- Price range
price > 10 AND price < 100

-- Date range
created_at >= '2024-01-01' AND created_at < '2024-02-01'
```

### Multiple Values
```sql
-- Instead of multiple ORs
status = 'active' OR status = 'pending' OR status = 'approved'

-- Use IN operator
status IN ('active', 'pending', 'approved')
```

### Exclusions
```sql
-- Exclude test data
email NOT LIKE '%@test.com' AND email NOT LIKE '%@example.com'

-- Exclude multiple statuses
status NOT IN ('deleted', 'archived', 'suspended')
```

### Complex Business Rules
```sql
-- VIP customers or high spenders
(customer_type = 'vip' OR total_spent > 10000) AND account_status = 'active'

-- Employees eligible for review
hire_date < '2023-01-01' AND performance_score >= 3.5 AND status = 'active'

-- Products needing restock
(quantity <= reorder_level OR quantity < 10) AND discontinued = 'false'
```

## 🔍 Real-World Examples

### E-commerce
```sql
-- High-value orders
order_total > 500 AND status IN ('paid', 'shipped')

-- Abandoned carts
status = 'cart' AND updated_at < '2024-01-01'

-- Premium products on sale
category = 'premium' AND discount_percentage > 0
```

### HR/Employee Data
```sql
-- Senior employees in tech
department IN ('engineering', 'IT') AND years_experience > 5

-- Employees due for review
last_review_date < '2023-07-01' AND status = 'active'

-- Remote workers in specific time zones
work_location = 'remote' AND timezone IN ('PST', 'MST')
```

### Electric Vehicles (Real Example)
```sql
-- Tesla vehicles with long range
Make = 'TESLA' AND Electric_Range > 300

-- New EVs in Seattle
Model_Year >= 2023 AND City = 'Seattle' AND Electric_Vehicle_Type = 'Battery Electric Vehicle (BEV)'

-- Eligible for incentives
Clean_Alternative_Fuel_Vehicle_Eligibility = 'Clean Alternative Fuel Vehicle Eligible' AND Model_Year >= 2022
```

## ⚠️ Common Mistakes

### ❌ Wrong: Using = with NULL
```sql
-- WRONG
email = NULL
manager_id != NULL

-- CORRECT
email IS NULL
manager_id IS NOT NULL
```

### ❌ Wrong: Forgetting quotes on strings
```sql
-- WRONG
status = active
city = Seattle

-- CORRECT
status = 'active'
city = 'Seattle'
```

### ❌ Wrong: Using double quotes
```sql
-- WRONG
name = "John"

-- CORRECT
name = 'John'
```

### ❌ Wrong: Empty IN lists
```sql
-- WRONG
department IN ()

-- CORRECT
department IN ('sales', 'marketing')
```

### ❌ Wrong: Unescaped quotes
```sql
-- WRONG
name = 'O'Brien'

-- CORRECT
name = 'O''Brien'
```

## 🏗️ Building Complex Expressions

### Step 1: Start Simple
```sql
status = 'active'
```

### Step 2: Add Conditions
```sql
status = 'active' AND department = 'sales'
```

### Step 3: Group Related Conditions
```sql
status = 'active' AND (department = 'sales' OR department = 'marketing')
```

### Step 4: Add More Logic
```sql
status = 'active' AND (department = 'sales' OR department = 'marketing') AND hire_date < '2024-01-01'
```

## 🎮 Practice Exercises

Try writing filters for these scenarios:

1. **Active users over 21**
   <details>
   <summary>Answer</summary>
   
   ```sql
   age > 21 AND status = 'active'
   ```
   </details>

2. **Orders from California or Nevada over $100**
   <details>
   <summary>Answer</summary>
   
   ```sql
   state IN ('CA', 'NV') AND order_total > 100
   ```
   </details>

3. **Employees without managers in the sales department**
   <details>
   <summary>Answer</summary>
   
   ```sql
   manager_id IS NULL AND department = 'sales'
   ```
   </details>

4. **Products with 'phone' in the name but not 'smartphone'**
   <details>
   <summary>Answer</summary>
   
   ```sql
   name LIKE '%phone%' AND name NOT LIKE '%smartphone%'
   ```
   </details>

5. **Users who signed up in 2023 with Gmail or Yahoo email**
   <details>
   <summary>Answer</summary>
   
   ```sql
   created_at >= '2023-01-01' AND created_at < '2024-01-01' AND (email LIKE '%@gmail.com' OR email LIKE '%@yahoo.com')
   ```
   </details>

## 💡 Pro Tips

1. **Use IN for multiple values** instead of chaining ORs
2. **Group conditions with parentheses** for clarity
3. **Test with simple filters first** then build complexity
4. **Use ILIKE for user-input searches** to be case-insensitive
5. **Be explicit with date ranges** using >= and < 
6. **NULL is special** - always use IS NULL/IS NOT NULL

## 🚫 Limitations

1. **No functions**: Can't use SQL functions like UPPER(), LOWER(), etc.
2. **No arithmetic**: Can't do `age + 5 > 30`
3. **No subqueries**: Can't reference other queries
4. **No BETWEEN**: Use `>= AND <=` instead
5. **Max length**: Expressions limited to 1000 characters
6. **Max depth**: Maximum 10 levels of nested parentheses

## 🔗 Quick Reference Card

```sql
-- Comparisons
=  !=  <>  >  <  >=  <=

-- Lists
IN ('a', 'b')  NOT IN ('x', 'y')

-- Patterns
LIKE '%pattern%'  NOT LIKE 'pattern'
ILIKE '%pattern%'  NOT ILIKE 'pattern'

-- NULL
IS NULL  IS NOT NULL

-- Logic
AND  OR  ()

-- Strings need quotes
'string value'

-- Escape quotes by doubling
'O''Brien'

-- Numbers don't need quotes
123  45.67

-- Dates as strings
'2024-01-01'
```