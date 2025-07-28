# 🎓 Interactive Filter Language Tutorial

Welcome to the Filter Expression Language tutorial! We'll start simple and build up to complex expressions.

## 📚 Lesson 1: Basic Comparisons

### Your First Filter
Let's start with the simplest filter - checking if someone is over 18:

```sql
age > 18
```

**Try it yourself:** Write a filter for users under 65:
<details>
<summary>Show Answer</summary>

```sql
age < 65
```
</details>

### String Equality
Strings always need **single quotes**:

```sql
status = 'active'
```

**Try it yourself:** Write a filter for users in the 'sales' department:
<details>
<summary>Show Answer</summary>

```sql
department = 'sales'
```
</details>

### 🏃 Practice Round 1
Write filters for:

1. Price greater than or equal to 100
2. City equals 'Seattle'  
3. Quantity less than 10

<details>
<summary>Show Answers</summary>

```sql
1. price >= 100
2. city = 'Seattle'
3. quantity < 10
```
</details>

---

## 📚 Lesson 2: Combining Conditions

### Using AND
Both conditions must be true:

```sql
age > 18 AND status = 'active'
```

This finds active users over 18.

**Try it yourself:** Find employees in 'IT' department with salary over 70000:
<details>
<summary>Show Answer</summary>

```sql
department = 'IT' AND salary > 70000
```
</details>

### Using OR
At least one condition must be true:

```sql
department = 'sales' OR department = 'marketing'
```

**Try it yourself:** Find products that are either 'featured' or have price under 20:
<details>
<summary>Show Answer</summary>

```sql
status = 'featured' OR price < 20
```
</details>

### 🏃 Practice Round 2
Write filters for:

1. Active users in California
2. Orders over $500 or priority shipping
3. Employees hired after 2020 with salary over 60000

<details>
<summary>Show Answers</summary>

```sql
1. status = 'active' AND state = 'CA'
2. total > 500 OR shipping = 'priority'
3. hire_date > '2020-12-31' AND salary > 60000
```
</details>

---

## 📚 Lesson 3: Using Parentheses

### Grouping Conditions
Parentheses control the order of operations:

```sql
status = 'active' AND (role = 'admin' OR role = 'manager')
```

This is different from:
```sql
(status = 'active' AND role = 'admin') OR role = 'manager'
```

**Try it yourself:** Find active users who are either VIP or have spent over $1000:
<details>
<summary>Show Answer</summary>

```sql
status = 'active' AND (type = 'vip' OR total_spent > 1000)
```
</details>

### 🏃 Practice Round 3
Write filters for:

1. Products in electronics or computers category with price over 100
2. Employees who are (seniors in engineering) or (anyone in management)

<details>
<summary>Show Answers</summary>

```sql
1. (category = 'electronics' OR category = 'computers') AND price > 100
2. (level = 'senior' AND department = 'engineering') OR department = 'management'
```
</details>

---

## 📚 Lesson 4: The IN Operator

### Checking Multiple Values
Instead of multiple ORs:
```sql
-- Tedious way
status = 'pending' OR status = 'processing' OR status = 'shipped'

-- Better way with IN
status IN ('pending', 'processing', 'shipped')
```

**Try it yourself:** Find users from California, Nevada, or Oregon:
<details>
<summary>Show Answer</summary>

```sql
state IN ('CA', 'NV', 'OR')
```
</details>

### Using NOT IN
Exclude multiple values:
```sql
status NOT IN ('deleted', 'archived', 'suspended')
```

### 🏃 Practice Round 4
Write filters for:

1. Products in categories: electronics, computers, phones
2. Users NOT in test, demo, or internal type
3. Orders from NY, NJ, or CT with total over 100

<details>
<summary>Show Answers</summary>

```sql
1. category IN ('electronics', 'computers', 'phones')
2. type NOT IN ('test', 'demo', 'internal')
3. state IN ('NY', 'NJ', 'CT') AND total > 100
```
</details>

---

## 📚 Lesson 5: Pattern Matching with LIKE

### Wildcards
- `%` matches any characters (including none)
- `_` matches exactly one character

### Common Patterns
```sql
-- Ends with @gmail.com
email LIKE '%@gmail.com'

-- Starts with John
name LIKE 'John%'

-- Contains admin anywhere
role LIKE '%admin%'

-- Exactly 5 characters starting with A
code LIKE 'A____'
```

**Try it yourself:** Find products with 'phone' in the name:
<details>
<summary>Show Answer</summary>

```sql
name LIKE '%phone%'
```
</details>

### Case-Insensitive with ILIKE
```sql
-- Matches john, John, JOHN, etc.
name ILIKE 'john%'
```

### 🏃 Practice Round 5
Write filters for:

1. Email addresses ending with your company domain (@company.com)
2. Phone numbers starting with 555
3. Descriptions containing 'urgent' (case-insensitive)

<details>
<summary>Show Answers</summary>

```sql
1. email LIKE '%@company.com'
2. phone LIKE '555%'
3. description ILIKE '%urgent%'
```
</details>

---

## 📚 Lesson 6: Working with NULL

### NULL is Special
```sql
-- WRONG - This won't work!
email = NULL

-- CORRECT
email IS NULL
```

### Checking for Values
```sql
-- Has a manager
manager_id IS NOT NULL

-- No phone number
phone IS NULL
```

### 🏃 Practice Round 6
Write filters for:

1. Employees without managers
2. Products with descriptions
3. Users with no email OR email is empty string

<details>
<summary>Show Answers</summary>

```sql
1. manager_id IS NULL
2. description IS NOT NULL
3. email IS NULL OR email = ''
```
</details>

---

## 📚 Lesson 7: Complex Real-World Examples

### E-commerce Example
Find high-value customers who recently made purchases:
```sql
total_purchases > 5000 
AND last_purchase_date > '2024-01-01' 
AND status = 'active' 
AND email NOT LIKE '%@test.com'
```

### HR Example
Find employees eligible for review:
```sql
(tenure_years > 1 OR role = 'contractor') 
AND last_review_date < '2023-07-01' 
AND status = 'active' 
AND department NOT IN ('temp', 'intern')
```

### 🏃 Final Challenge
Write filters for these scenarios:

1. **Active premium users**: Status is active AND (plan is 'premium' OR 'enterprise'), registered before 2024

2. **Products needing restock**: Quantity less than 10 OR quantity less than reorder_level, AND not discontinued

3. **High-risk transactions**: Amount over 10000 OR (amount over 5000 AND country not in US, CA, UK), AND user not verified

<details>
<summary>Show Answers</summary>

```sql
1. status = 'active' AND plan IN ('premium', 'enterprise') AND registered_date < '2024-01-01'

2. (quantity < 10 OR quantity < reorder_level) AND discontinued = 'false'

3. (amount > 10000 OR (amount > 5000 AND country NOT IN ('US', 'CA', 'UK'))) AND verified = 'false'
```
</details>

---

## 🎯 Quick Reference

### Remember These Rules
1. **Strings need quotes**: `'value'`
2. **Numbers don't**: `123`
3. **NULL is special**: `IS NULL` / `IS NOT NULL`
4. **Use IN for lists**: `IN ('a', 'b', 'c')`
5. **Escape quotes by doubling**: `'O''Brien'`
6. **Parentheses for grouping**: `(A OR B) AND C`

### Common Mistakes to Avoid
- ❌ `status = active` → ✅ `status = 'active'`
- ❌ `age > '25'` → ✅ `age > 25`
- ❌ `email = NULL` → ✅ `email IS NULL`
- ❌ `name = "John"` → ✅ `name = 'John'`

## 🏆 Congratulations!

You've learned the filter expression language! You can now:
- ✅ Write basic comparisons
- ✅ Combine conditions with AND/OR
- ✅ Use parentheses for complex logic
- ✅ Filter with IN/NOT IN
- ✅ Match patterns with LIKE
- ✅ Handle NULL values
- ✅ Build complex real-world filters

### Next Steps
1. Practice with your own data
2. Start simple and build complexity
3. Test filters with small samples first
4. Refer to the cheat sheet when needed

Happy filtering! 🎉