# 🔧 Filter Expression Troubleshooting Guide

## 🚨 Common Error Messages and Solutions

### Error: "Invalid column: [column_name]"
**Cause**: The column doesn't exist in your dataset

**Solutions**:
```sql
-- Wrong column name
products = 'laptop'  ❌

-- Check the actual column name (might be different case or spelling)
product = 'laptop'   ✓
Product = 'laptop'   ✓
product_name = 'laptop'  ✓
```

**How to find correct column names**:
1. Get a sample of data first without filters
2. Check the schema documentation
3. Column names are case-sensitive

---

### Error: "Expected value at position X"
**Cause**: Syntax error, usually missing quotes or value

**Examples**:
```sql
-- Forgot quotes on string
status = active  ❌
status = 'active'  ✓

-- Forgot value after operator
age >   ❌
age > 25  ✓

-- Using boolean literals without quotes
is_active = true  ❌
is_active = 'true'  ✓
```

---

### Error: "Expected ')' at position X"
**Cause**: Unmatched parentheses

**Examples**:
```sql
-- Missing closing parenthesis
(age > 25 AND status = 'active'  ❌
(age > 25 AND status = 'active')  ✓

-- Extra parenthesis
age > 25) AND status = 'active'  ❌
age > 25 AND status = 'active'  ✓
```

**Tip**: Count your parentheses - equal number of ( and )

---

### Error: "IN list cannot be empty"
**Cause**: Empty list in IN operator

**Examples**:
```sql
-- Empty IN list
department IN ()  ❌

-- Must have at least one value
department IN ('sales')  ✓
department IN ('sales', 'marketing')  ✓
```

---

### Error: "Invalid operator: [operator]"
**Cause**: Using an unsupported operator

**Examples**:
```sql
-- Wrong operator
age => 25  ❌
age >= 25  ✓

-- BETWEEN not supported
age BETWEEN 25 AND 65  ❌
age >= 25 AND age <= 65  ✓

-- CONTAINS not supported
name CONTAINS 'john'  ❌
name LIKE '%john%'  ✓
```

---

## 🐛 Common Logic Errors (No Error Message)

### Getting No Results When You Should

**Problem 1: Wrong AND/OR logic**
```sql
-- This finds nobody (status can't be both)
status = 'active' AND status = 'pending'  ❌

-- You probably meant OR
status = 'active' OR status = 'pending'  ✓
-- Or better:
status IN ('active', 'pending')  ✓
```

**Problem 2: Impossible date ranges**
```sql
-- End date before start date
created_at > '2024-01-01' AND created_at < '2023-01-01'  ❌

-- Correct date range
created_at > '2023-01-01' AND created_at < '2024-01-01'  ✓
```

**Problem 3: NULL comparison**
```sql
-- This won't find NULL values
manager_id = NULL  ❌
manager_id != NULL  ❌

-- Correct NULL check
manager_id IS NULL  ✓
manager_id IS NOT NULL  ✓
```

---

### Getting Too Many Results

**Problem 1: Missing parentheses**
```sql
-- This matches ALL managers (regardless of status)
status = 'active' AND role = 'user' OR role = 'manager'  ❌

-- Add parentheses to group OR conditions
status = 'active' AND (role = 'user' OR role = 'manager')  ✓
```

**Problem 2: NOT IN confusion**
```sql
-- This is always true (if status is 'active', it's definitely NOT 'deleted')
status = 'active' OR status NOT IN ('deleted', 'archived')  ❌

-- You probably meant AND
status = 'active' AND status NOT IN ('deleted', 'archived')  ✓
-- (Though the NOT IN is redundant here)
```

---

## 🔍 Debugging Strategies

### 1. Start Simple, Build Up
```sql
-- Start with one condition
department = 'sales'

-- Add another
department = 'sales' AND status = 'active'

-- Add more complexity
department = 'sales' AND status = 'active' AND (role = 'manager' OR years > 5)
```

### 2. Test Each Part Separately
If this doesn't work:
```sql
(age > 25 AND department = 'sales') OR (age > 30 AND department = 'marketing')
```

Test each part:
1. `age > 25 AND department = 'sales'`
2. `age > 30 AND department = 'marketing'`

### 3. Check Your Data Types
```sql
-- If age is stored as string (check your schema!)
age > '25'  ✓

-- If it's numeric
age > 25  ✓

-- Same with booleans - might be stored as strings
is_verified = 'true'  ✓
is_verified = true  ❌ (unless your system supports boolean literals)
```

---

## 💡 Pro Debugging Tips

### Use Sample Data First
Create a filter that should return just a few known records:
```sql
-- Test with a specific ID first
id = '12345'

-- Then add your conditions
id = '12345' AND status = 'active'

-- Remove the ID check when working
status = 'active'
```

### Check Case Sensitivity
```sql
-- These might be different
Status = 'Active'
status = 'active'
STATUS = 'ACTIVE'

-- Use ILIKE for case-insensitive
status ILIKE 'active'
```

### Verify String Values
```sql
-- Watch for extra spaces
status = 'active'   -- No spaces
status = 'active '  -- Trailing space
status = ' active'  -- Leading space

-- Trim in your data or use LIKE
status LIKE '%active%'
```

---

## 📊 Performance Issues

### Slow Filters

**Problem**: LIKE with leading wildcard
```sql
-- Slow (can't use index)
email LIKE '%@gmail.com'

-- Faster (if you have an index on email)
email LIKE 'john%'
```

**Problem**: Too many OR conditions
```sql
-- Slower
id = 1 OR id = 2 OR id = 3 OR ... OR id = 100

-- Faster
id IN (1, 2, 3, ..., 100)
```

---

## 🆘 Still Stuck?

### Checklist:
1. ✓ Are all string values in single quotes?
2. ✓ Are numeric values NOT in quotes?
3. ✓ Do opening and closing parentheses match?
4. ✓ Are column names spelled correctly (case-sensitive)?
5. ✓ Are you using IS NULL for null checks?
6. ✓ Is your date format correct (YYYY-MM-DD)?

### Get Sample Working Filter:
Start with a filter that definitely works:
```sql
1 = 1  -- Always true
```

Then modify it step by step:
```sql
1 = 1 AND status = 'active'
```

### Common Working Patterns:
```sql
-- String equality
column = 'value'

-- Number comparison  
column > 123

-- List membership
column IN ('a', 'b', 'c')

-- Pattern match
column LIKE '%text%'

-- Null check
column IS NOT NULL

-- Date comparison
column > '2024-01-01'
```

If none of these patterns work, the issue might be:
- Wrong column name
- No data matching your criteria
- Data type mismatch
- Permission issues

Remember: Start simple, test often, build complexity gradually! 🚀