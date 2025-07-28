# Filter Expression Implementation Guide

## Overview

This guide provides a comprehensive technical overview of the SQL-like filter expression implementation in the DSA sampling system. The implementation allows users to specify complex filtering conditions using a text-based expression language instead of structured JSON arrays.

## Architecture

### Core Components

```
┌─────────────────────────┐
│     API Layer           │
│  (sampling.py)          │
│  - FilterSpec model     │
│  - Request validation   │
└───────────┬─────────────┘
            │
┌───────────▼─────────────┐
│   Sampling Executor     │
│ (sampling_executor.py)  │
│  - Filter integration   │
│  - SQL query builder    │
└───────────┬─────────────┘
            │
┌───────────▼─────────────┐
│    Filter Parser        │
│  (filter_parser.py)     │
│  - Tokenizer            │
│  - AST builder          │
│  - SQL generator        │
└─────────────────────────┘
```

## Filter Parser Implementation

### 1. Tokenization

The parser uses a regex-based tokenizer that recognizes:

```python
TOKEN_PATTERNS = [
    (TokenType.LPAREN, r'\('),
    (TokenType.RPAREN, r'\)'),
    (TokenType.AND, r'\bAND\b'),
    (TokenType.OR, r'\bOR\b'),
    (TokenType.OPERATOR, r'(IS\s+NOT\s+NULL|IS\s+NULL|NOT\s+LIKE|NOT\s+ILIKE|NOT\s+IN|>=|<=|!=|<>|=|>|<|IN|LIKE|ILIKE)'),
    (TokenType.STRING, r"'([^']*)'"),
    (TokenType.NUMBER, r'-?\d+(\.\d+)?'),
    (TokenType.IDENTIFIER, r'"([^"]+)"|[a-zA-Z_][a-zA-Z0-9_]*'),  # Quoted or regular identifiers
    (TokenType.COMMA, r','),
]
```

Key features:
- **Quoted Identifiers**: Supports `"column name"` for columns with spaces
- **String Literals**: Uses single quotes `'value'`
- **Case Insensitive**: Keywords are case-insensitive
- **Number Support**: Integer and decimal numbers

### 2. Parsing Process

The parser uses recursive descent parsing with operator precedence:

```
Expression → OrExpression
OrExpression → AndExpression ( OR AndExpression )*
AndExpression → PrimaryExpression ( AND PrimaryExpression )*
PrimaryExpression → Condition | ( Expression )
Condition → Identifier Operator Value
```

### 3. Abstract Syntax Tree (AST)

The parser builds an AST with these node types:

```python
@dataclass
class BinaryExpression(Expression):
    left: Expression
    operator: str  # "AND" or "OR"
    right: Expression

@dataclass
class ConditionExpression(Expression):
    condition: Condition

@dataclass
class Condition:
    column: str
    operator: str
    value: Any
```

### 4. SQL Generation

The AST is converted to parameterized SQL with security features:

```python
def to_sql(expr, valid_columns, column_types, param_start=1):
    # Validates columns against schema
    # Generates parameterized queries
    # Applies appropriate type casting
    # Returns (sql_string, parameters)
```

## Security Features

### 1. Column Validation

All column names are validated against the table schema:

```python
if cond.column not in valid_columns:
    raise ValueError(f"Invalid column: {cond.column}")
```

### 2. Operator Whitelist

Only allowed operators are accepted:

```python
ALLOWED_OPERATORS = {
    '>', '>=', '<', '<=', '=', '!=', '<>',
    'IN', 'NOT IN', 'LIKE', 'ILIKE', 
    'NOT LIKE', 'NOT ILIKE',
    'IS NULL', 'IS NOT NULL'
}
```

### 3. Parameterized Queries

All values are passed as parameters, preventing SQL injection:

```python
# Generated SQL uses parameters
"((data->>'Model Year')::text >= $1)"
# Parameters: ['2022']
```

### 4. Input Validation

- Maximum expression length (1000 characters)
- Maximum nesting depth (10 levels)
- Column name pattern validation
- Empty IN list validation

## Data Type Handling

### Type Casting

The parser applies PostgreSQL type casts based on column types:

```python
type_map = {
    'integer': '::integer',
    'bigint': '::bigint',
    'numeric': '::numeric',
    'float': '::float',
    'boolean': '::boolean',
    'date': '::date',
    'timestamp': '::timestamp',
    'text': '',  # No cast for text
}
```

### Nested Data Structure

The implementation handles DSA's nested JSON structure:

```sql
-- Extracts data from either nested or flat structure
CASE 
    WHEN r.data ? 'data' THEN r.data->'data'->>'column_name'
    ELSE r.data->>'column_name'
END
```

## API Integration

### Request Format

```json
{
    "source_ref": "main",
    "table_key": "primary",
    "rounds": [{
        "round_number": 1,
        "method": "random",
        "parameters": {"sample_size": 100},
        "filters": {
            "expression": "\"Model Year\" >= '2022' AND Make = 'TESLA'"
        }
    }]
}
```

### Processing Flow

1. **API Validation**: FilterSpec validates expression exists
2. **Job Creation**: Expression passed to sampling executor
3. **SQL Building**: Parser converts expression to SQL
4. **Query Execution**: Parameterized query filters results
5. **Sampling**: Filtered data is sampled per method

## Supported Operators

### Comparison Operators
- `=` - Equality
- `!=`, `<>` - Not equal
- `>`, `>=` - Greater than (or equal)
- `<`, `<=` - Less than (or equal)

### List Operators
- `IN ('value1', 'value2')` - Value in list
- `NOT IN ('value1', 'value2')` - Value not in list

### Pattern Matching
- `LIKE 'pattern'` - Case-sensitive pattern match
- `ILIKE 'pattern'` - Case-insensitive pattern match
- `NOT LIKE 'pattern'` - Negated pattern match
- `NOT ILIKE 'pattern'` - Negated case-insensitive

### NULL Checking
- `IS NULL` - Check for NULL
- `IS NOT NULL` - Check for non-NULL

## Implementation Examples

### Basic Filter
```python
# Expression
"age > 25"

# Generated SQL
"((data->>'age')::text > $1)"
# Parameters: [25]
```

### Complex Filter with Quoted Columns
```python
# Expression
"(\"Model Year\" >= '2022' OR Make = 'RIVIAN') AND \"Electric Range\" > '200'"

# Generated SQL
"(((data->>'Model Year')::text >= $1) OR ((data->>'Make')::text = $2)) AND ((data->>'Electric Range')::text > $3)"
# Parameters: ['2022', 'RIVIAN', '200']
```

### IN Operator
```python
# Expression
"County IN ('King', 'Pierce', 'Snohomish')"

# Generated SQL
"((data->>'County')::text IN ($1, $2, $3))"
# Parameters: ['King', 'Pierce', 'Snohomish']
```

## Error Handling

### Parser Errors

```python
# Invalid column
ValueError: Invalid column: NonExistentColumn

# Invalid operator
ValueError: Invalid operator: CONTAINS

# Syntax errors
ValueError: Expected ')' at position 45

# Invalid characters
ValueError: Invalid character at position 20: '@'
```

### Common Issues

1. **Spaces in Column Names**
   - Solution: Use double quotes `"Model Year"`

2. **String Values**
   - Always use single quotes for values
   - Numbers can be quoted or unquoted

3. **Case Sensitivity**
   - Keywords (AND, OR, IN) are case-insensitive
   - Column names preserve case
   - String values are case-sensitive

## Performance Considerations

1. **Index Usage**
   - JSON operators can use GIN indexes
   - Type casting may affect index usage
   - Consider functional indexes for frequently filtered columns

2. **Query Optimization**
   - Parser generates minimal SQL
   - Parameterized queries enable plan caching
   - Nested data extraction is optimized

3. **Memory Usage**
   - Limited expression length prevents excessive memory use
   - AST depth limited to prevent stack overflow
   - Efficient tokenization without backtracking

## Testing Strategy

### Unit Tests
- Tokenizer edge cases
- Parser syntax validation
- SQL generation correctness
- Security validation

### Integration Tests
- End-to-end API testing
- Various sampling methods with filters
- Performance benchmarks
- Error propagation

### Test Data
- Use consistent test datasets
- Cover all data types
- Include edge cases (NULLs, empty strings)
- Test with real-world column names

## Future Enhancements

### Potential Improvements

1. **Additional Operators**
   - BETWEEN for range queries
   - Regular expression support
   - JSON path operators

2. **Function Support**
   - Date/time functions
   - String manipulation
   - Mathematical functions

3. **Performance Optimizations**
   - Query plan caching
   - Filter predicate pushdown
   - Parallel execution

4. **Developer Experience**
   - Filter expression validation endpoint
   - Query plan explanation
   - Performance metrics

## Conclusion

The filter expression implementation provides a secure, flexible, and performant way to filter data during sampling operations. The SQL-like syntax is familiar to users while the robust parsing and validation ensure safety and correctness.

Key achievements:
- ✅ SQL-like expression syntax
- ✅ Support for complex boolean logic
- ✅ Quoted identifiers for spaces
- ✅ Type-safe SQL generation
- ✅ SQL injection prevention
- ✅ Integration with all sampling methods

The implementation balances user-friendliness with security, making it suitable for production use in data sampling workflows.