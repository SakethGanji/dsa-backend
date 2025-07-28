# Filter Expression UI Guide

## Quick Start for UI Implementation

This guide helps UI developers implement filter expression inputs in their applications.

## Basic UI Components

### 1. Filter Input Field

```jsx
// React example
<FilterInput>
  <label>Filter Expression</label>
  <textarea
    placeholder='e.g., age > 25 AND status = "active"'
    value={filterExpression}
    onChange={handleFilterChange}
    rows={3}
  />
  <small>Use SQL-like syntax with AND/OR operators</small>
</FilterInput>
```

### 2. Syntax Helper Component

```jsx
const FilterSyntaxHelper = () => (
  <div className="syntax-helper">
    <h4>Quick Syntax Reference</h4>
    <ul>
      <li><code>column = 'value'</code> - Exact match</li>
      <li><code>column > 100</code> - Greater than</li>
      <li><code>"column name" = 'value'</code> - Columns with spaces</li>
      <li><code>column IN ('A', 'B', 'C')</code> - Multiple values</li>
      <li><code>column LIKE '%pattern%'</code> - Pattern matching</li>
      <li><code>condition1 AND condition2</code> - Both conditions</li>
      <li><code>condition1 OR condition2</code> - Either condition</li>
      <li><code>(condition1 OR condition2) AND condition3</code> - Grouping</li>
    </ul>
  </div>
);
```

## UI Patterns

### 1. Simple Filter Builder

For basic users who need guided input:

```jsx
const SimpleFilterBuilder = () => {
  const [filters, setFilters] = useState([]);
  
  const addFilter = () => {
    setFilters([...filters, { column: '', operator: '=', value: '' }]);
  };
  
  const buildExpression = () => {
    return filters
      .map(f => `${f.column} ${f.operator} '${f.value}'`)
      .join(' AND ');
  };
  
  return (
    <div>
      {filters.map((filter, idx) => (
        <FilterRow key={idx}>
          <select value={filter.column} onChange={...}>
            <option value="">Select column...</option>
            <option value="age">Age</option>
            <option value="status">Status</option>
            <option value="Model Year">Model Year</option>
          </select>
          
          <select value={filter.operator} onChange={...}>
            <option value="=">=</option>
            <option value="!=">≠</option>
            <option value=">">></option>
            <option value="<"><</option>
            <option value="LIKE">contains</option>
          </select>
          
          <input type="text" value={filter.value} onChange={...} />
        </FilterRow>
      ))}
      
      <button onClick={addFilter}>+ Add Filter</button>
      <code>{buildExpression()}</code>
    </div>
  );
};
```

### 2. Advanced Expression Editor

For power users who want full control:

```jsx
const AdvancedFilterEditor = () => {
  const [expression, setExpression] = useState('');
  const [error, setError] = useState('');
  const [isValid, setIsValid] = useState(true);
  
  // Syntax highlighting
  const highlightSyntax = (text) => {
    return text
      .replace(/\b(AND|OR|IN|LIKE|ILIKE|NOT|IS|NULL)\b/gi, '<span class="keyword">$1</span>')
      .replace(/'[^']*'/g, '<span class="string">$&</span>')
      .replace(/\b\d+(\.\d+)?\b/g, '<span class="number">$&</span>')
      .replace(/"[^"]+"/g, '<span class="column">$&</span>');
  };
  
  return (
    <div className="filter-editor">
      <div className="editor-wrapper">
        <div 
          className="syntax-highlight"
          dangerouslySetInnerHTML={{ __html: highlightSyntax(expression) }}
        />
        <textarea
          value={expression}
          onChange={(e) => setExpression(e.target.value)}
          onBlur={validateExpression}
          spellCheck={false}
        />
      </div>
      {error && <div className="error">{error}</div>}
    </div>
  );
};
```

## Common UI Features

### 1. Auto-complete for Columns

```jsx
const ColumnAutocomplete = ({ columns, value, onChange }) => {
  const [suggestions, setSuggestions] = useState([]);
  
  const handleInput = (text) => {
    const filtered = columns.filter(col => 
      col.toLowerCase().includes(text.toLowerCase())
    );
    setSuggestions(filtered);
    onChange(text);
  };
  
  return (
    <div className="autocomplete">
      <input value={value} onChange={(e) => handleInput(e.target.value)} />
      {suggestions.length > 0 && (
        <ul className="suggestions">
          {suggestions.map(col => (
            <li onClick={() => onChange(col)}>
              {col.includes(' ') ? `"${col}"` : col}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
};
```

### 2. Expression Validation

```jsx
const validateFilterExpression = async (expression) => {
  try {
    const response = await fetch('/api/sampling/validate-filter', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ expression })
    });
    
    if (!response.ok) {
      const error = await response.json();
      return { valid: false, error: error.message };
    }
    
    return { valid: true };
  } catch (error) {
    return { valid: false, error: 'Validation failed' };
  }
};
```

### 3. Filter Templates

```jsx
const FilterTemplates = ({ onSelect }) => {
  const templates = [
    { name: 'Recent Active Users', expression: 'status = "active" AND last_login > "2024-01-01"' },
    { name: 'High Value Customers', expression: 'total_purchases > 1000 AND customer_tier = "gold"' },
    { name: 'New Tesla Vehicles', expression: 'Make = "TESLA" AND "Model Year" >= "2022"' },
  ];
  
  return (
    <div className="filter-templates">
      <h4>Common Filters</h4>
      {templates.map(template => (
        <button onClick={() => onSelect(template.expression)}>
          {template.name}
        </button>
      ))}
    </div>
  );
};
```

## Error Handling

### User-Friendly Error Messages

```jsx
const formatError = (error) => {
  const errorMap = {
    'Invalid column': 'Column not found. Use quotes for names with spaces.',
    'Expected operator': 'Missing operator (=, >, <, etc.) after column name.',
    'Expected value': 'Missing value after operator.',
    'Unclosed parenthesis': 'Missing closing parenthesis ).',
    'Empty IN list': 'IN operator requires at least one value.',
  };
  
  for (const [key, message] of Object.entries(errorMap)) {
    if (error.includes(key)) {
      return message;
    }
  }
  
  return error;
};
```

## Best Practices

### 1. Progressive Disclosure

Start simple, reveal complexity as needed:

```jsx
const FilterInterface = () => {
  const [mode, setMode] = useState('simple'); // 'simple' | 'advanced'
  
  return (
    <div>
      <ToggleButtons>
        <button onClick={() => setMode('simple')}>Simple</button>
        <button onClick={() => setMode('advanced')}>Advanced</button>
      </ToggleButtons>
      
      {mode === 'simple' ? <SimpleFilterBuilder /> : <AdvancedFilterEditor />}
    </div>
  );
};
```

### 2. Real-time Feedback

```jsx
const FilterWithPreview = () => {
  const [expression, setExpression] = useState('');
  const [preview, setPreview] = useState({ count: 0, sample: [] });
  
  // Debounced preview
  useEffect(() => {
    const timer = setTimeout(() => {
      if (expression) {
        fetchPreview(expression).then(setPreview);
      }
    }, 500);
    
    return () => clearTimeout(timer);
  }, [expression]);
  
  return (
    <div>
      <FilterInput value={expression} onChange={setExpression} />
      <div className="preview">
        <p>{preview.count} rows match this filter</p>
        <SampleDataTable data={preview.sample} />
      </div>
    </div>
  );
};
```

### 3. Mobile Considerations

```jsx
const MobileFilterInput = () => {
  return (
    <div className="mobile-filter">
      {/* Larger touch targets */}
      <select className="filter-preset">
        <option>Custom filter...</option>
        <option>Active users</option>
        <option>Recent activity</option>
      </select>
      
      {/* Simplified syntax */}
      <div className="quick-filters">
        <chip onClick={() => addToFilter('status = "active"')}>
          Active
        </chip>
        <chip onClick={() => addToFilter('"created_at" > "2024-01-01"')}>
          Recent
        </chip>
      </div>
      
      {/* Full-screen editor option */}
      <button onClick={openFullScreenEditor}>
        Edit Filter Expression
      </button>
    </div>
  );
};
```

## Integration Example

### Complete Filter Component

```jsx
const DataSamplingFilter = ({ onApply }) => {
  const [expression, setExpression] = useState('');
  const [isValid, setIsValid] = useState(true);
  const [error, setError] = useState('');
  const [showHelp, setShowHelp] = useState(false);
  
  const handleApply = () => {
    if (isValid && expression) {
      onApply({ expression });
    }
  };
  
  return (
    <Card>
      <CardHeader>
        <h3>Filter Data</h3>
        <IconButton onClick={() => setShowHelp(!showHelp)}>
          <HelpIcon />
        </IconButton>
      </CardHeader>
      
      <CardBody>
        <FilterInput
          value={expression}
          onChange={setExpression}
          error={error}
          placeholder="e.g., age > 25 AND city = 'Seattle'"
        />
        
        {showHelp && <FilterSyntaxHelper />}
        
        <FilterTemplates onSelect={setExpression} />
      </CardBody>
      
      <CardFooter>
        <Button onClick={handleApply} disabled={!isValid}>
          Apply Filter
        </Button>
      </CardFooter>
    </Card>
  );
};
```

## Tips for UI Developers

1. **Quote Column Names with Spaces**
   - Auto-add quotes when user selects columns with spaces
   - Show visual indicator for quoted columns

2. **Value Formatting**
   - Always quote string values in the expression
   - Numbers can be unquoted
   - Dates should be quoted: '2024-01-01'

3. **Operator Shortcuts**
   - Map user-friendly terms to SQL operators:
     - "equals" → "="
     - "contains" → "LIKE '%value%'"
     - "starts with" → "LIKE 'value%'"

4. **Visual Feedback**
   - Syntax highlighting for keywords
   - Different colors for columns, operators, values
   - Red underline for syntax errors

5. **Help Integration**
   - Tooltip on hover for operators
   - Examples in placeholder text
   - Link to full documentation

## Testing Checklist

- [ ] Valid expressions are accepted
- [ ] Invalid syntax shows clear errors
- [ ] Columns with spaces are properly quoted
- [ ] Auto-complete works for column names
- [ ] Templates apply correctly
- [ ] Expression validation provides feedback
- [ ] Mobile experience is optimized
- [ ] Accessibility standards are met

## Resources

- [Full Filter Language Guide](./FILTER_LANGUAGE_GUIDE.md)
- [API Documentation](./API_FILTER_GUIDE.md)
- [Troubleshooting Guide](./FILTER_TROUBLESHOOTING.md)