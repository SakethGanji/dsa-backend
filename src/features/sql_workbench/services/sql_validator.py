"""Unified SQL validation service for all SQL workbench operations."""
import re
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum


class ValidationLevel(Enum):
    """Validation levels for SQL queries."""
    SYNTAX = "syntax"
    SEMANTIC = "semantic"
    SECURITY = "security"
    PERFORMANCE = "performance"
    ALL = "all"


@dataclass
class ValidationResult:
    """Result of SQL validation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    referenced_tables: List[str]
    validation_level: ValidationLevel
    metadata: Dict[str, Any]


class SqlValidator:
    """Unified service for SQL query validation."""
    
    # Comprehensive list of disallowed SQL keywords for security
    DISALLOWED_KEYWORDS = [
        'DROP', 'CREATE', 'ALTER', 'TRUNCATE', 'DELETE', 'UPDATE', 
        'INSERT', 'GRANT', 'REVOKE', 'EXECUTE', 'CALL', 'EXEC',
        'MERGE', 'REPLACE', 'RENAME', 'COMMENT'
    ]
    
    # Performance-impacting patterns
    PERFORMANCE_WARNINGS = {
        r'SELECT\s+\*': "Using SELECT * may impact performance and should be avoided",
        r'NOT\s+IN\s*\(': "NOT IN can be slow with large datasets, consider NOT EXISTS",
        r'LIKE\s+[\'"]%': "Leading wildcard in LIKE pattern prevents index usage",
        r'OR\s+': "Multiple OR conditions may prevent index usage, consider UNION",
        r'DISTINCT': "DISTINCT can be expensive, ensure it's necessary"
    }
    
    async def validate(
        self,
        sql: str,
        sources: Optional[List[Dict[str, Any]]] = None,
        level: ValidationLevel = ValidationLevel.ALL
    ) -> ValidationResult:
        """
        Validate SQL query at specified level.
        
        Args:
            sql: SQL query to validate
            sources: Optional list of source configurations with aliases
            level: Validation level to apply
            
        Returns:
            ValidationResult with errors, warnings, and metadata
        """
        errors = []
        warnings = []
        referenced_tables = []
        metadata = {}
        
        # Normalize SQL for analysis
        sql_normalized = sql.strip()
        sql_upper = sql_normalized.upper()
        
        # Basic syntax validation
        if level in [ValidationLevel.SYNTAX, ValidationLevel.ALL]:
            syntax_errors = self._validate_syntax(sql_normalized)
            errors.extend(syntax_errors)
        
        # Security validation
        if level in [ValidationLevel.SECURITY, ValidationLevel.ALL]:
            security_errors = self._validate_security(sql_upper)
            errors.extend(security_errors)
        
        # Semantic validation (requires sources)
        if level in [ValidationLevel.SEMANTIC, ValidationLevel.ALL] and sources:
            semantic_errors, used_tables = self._validate_semantic(sql_normalized, sources)
            errors.extend(semantic_errors)
            referenced_tables.extend(used_tables)
        
        # Performance validation
        if level in [ValidationLevel.PERFORMANCE, ValidationLevel.ALL]:
            perf_warnings = self._validate_performance(sql_normalized)
            warnings.extend(perf_warnings)
        
        # Extract referenced tables
        tables = self._extract_referenced_tables(sql_normalized)
        referenced_tables.extend(tables)
        referenced_tables = list(set(referenced_tables))  # Remove duplicates
        
        # Add metadata
        metadata['query_length'] = len(sql_normalized)
        metadata['has_subqueries'] = '(' in sql_normalized and 'SELECT' in sql_upper
        metadata['has_joins'] = 'JOIN' in sql_upper
        metadata['has_aggregations'] = any(agg in sql_upper for agg in ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN'])
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            referenced_tables=referenced_tables,
            validation_level=level,
            metadata=metadata
        )
    
    def _validate_syntax(self, sql: str) -> List[str]:
        """Validate SQL syntax."""
        errors = []
        
        if not sql:
            errors.append("SQL query cannot be empty")
            return errors
        
        # Check for balanced parentheses
        if sql.count('(') != sql.count(')'):
            errors.append("Unbalanced parentheses in SQL query")
        
        # Check for balanced quotes
        single_quotes = sql.count("'")
        if single_quotes % 2 != 0:
            errors.append("Unbalanced single quotes in SQL query")
        
        double_quotes = sql.count('"')
        if double_quotes % 2 != 0:
            errors.append("Unbalanced double quotes in SQL query")
        
        # Check for semicolon (warning, not error)
        if not sql.rstrip().endswith(';'):
            # This is a warning, not an error, handled in performance validation
            pass
        
        # Check basic SELECT structure
        if 'SELECT' in sql.upper() and 'FROM' not in sql.upper():
            errors.append("SELECT statement missing FROM clause")
        
        return errors
    
    def _validate_security(self, sql_upper: str) -> List[str]:
        """Validate SQL for security issues."""
        errors = []
        
        # Check for disallowed keywords
        for keyword in self.DISALLOWED_KEYWORDS:
            if re.search(r'\b' + keyword + r'\b', sql_upper):
                errors.append(f"Disallowed operation '{keyword}' not permitted in transformations")
        
        # Check for SQL injection patterns
        if '--' in sql_upper:
            errors.append("SQL comments (--) are not allowed for security reasons")
        
        if '/*' in sql_upper or '*/' in sql_upper:
            errors.append("Block comments (/* */) are not allowed for security reasons")
        
        # Check for system tables/functions
        system_patterns = ['INFORMATION_SCHEMA', 'PG_', 'MYSQL.', 'SYS.']
        for pattern in system_patterns:
            if pattern in sql_upper:
                errors.append(f"Access to system tables/schemas ({pattern}) is not allowed")
        
        return errors
    
    def _validate_semantic(
        self, 
        sql: str, 
        sources: List[Dict[str, Any]]
    ) -> Tuple[List[str], List[str]]:
        """Validate semantic correctness with available sources."""
        errors = []
        used_tables = []
        
        # Get available aliases
        available_aliases = {source.get('alias', '') for source in sources}
        available_aliases.discard('')  # Remove empty strings
        
        # First, remove SQL functions and their contents to avoid false positives
        # This handles COUNT(), SUM(), json_extract(), etc.
        sql_cleaned = sql
        function_pattern = r'\b(?:COUNT|SUM|AVG|MAX|MIN|LENGTH|SUBSTR|SUBSTRING|TRIM|UPPER|LOWER|COALESCE|CAST|CONVERT|DATE|TIME|YEAR|MONTH|DAY|jsonb_extract_path|jsonb_extract_path_text|json_build_object|json_agg)\s*\([^)]*\)'
        sql_cleaned = re.sub(function_pattern, ' FUNCTION_PLACEHOLDER ', sql_cleaned, flags=re.IGNORECASE)
        
        # Remove string literals
        sql_cleaned = re.sub(r"'[^']*'", " STRING_LITERAL ", sql_cleaned)
        sql_cleaned = re.sub(r'"[^"]*"', " STRING_LITERAL ", sql_cleaned)
        
        # Remove numeric literals
        sql_cleaned = re.sub(r'\b\d+\b', " NUMBER ", sql_cleaned)
        
        # Find table references in SQL
        # More specific patterns that only match actual table positions
        table_patterns = [
            # FROM table or FROM table alias
            r'FROM\s+([a-zA-Z_]\w*)(?:\s+(?:AS\s+)?([a-zA-Z_]\w*))?',
            # JOIN table or JOIN table alias  
            r'(?:LEFT|RIGHT|INNER|OUTER|CROSS)?\s*JOIN\s+([a-zA-Z_]\w*)(?:\s+(?:AS\s+)?([a-zA-Z_]\w*))?',
            # Additional tables in FROM clause (after commas)
            r'FROM\s+[^,]+,\s*([a-zA-Z_]\w*)(?:\s+(?:AS\s+)?([a-zA-Z_]\w*))?'
        ]
        
        found_tables = []
        for pattern in table_patterns:
            matches = re.findall(pattern, sql_cleaned, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                table_name = match[0]
                alias = match[1] if len(match) > 1 and match[1] else None
                
                # Skip SQL keywords that might be caught
                sql_keywords = {'DUAL', 'VALUES', 'LATERAL', 'UNNEST', 'TABLE', 'ONLY'}
                if table_name.upper() in sql_keywords:
                    continue
                    
                found_tables.append((table_name, alias))
                used_tables.append(table_name)
        
        # Validate found tables against available sources
        for table_name, alias in found_tables:
            # Check if the table reference matches an available alias
            if table_name in available_aliases:
                continue
            elif alias and alias in available_aliases:
                # Table name doesn't matter if alias matches
                continue
            else:
                # Check if it might be a CTE or subquery
                if not self._is_cte_or_subquery(sql, table_name):
                    errors.append(f"Table '{table_name}' not found in available sources")
        
        # Check for column references that look like table.column
        # but where 'table' is actually an alias
        column_ref_pattern = r'([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)'
        column_refs = re.findall(column_ref_pattern, sql_cleaned)
        for table_ref, column_name in column_refs:
            if table_ref not in available_aliases and table_ref not in [t[1] for t in found_tables if t[1]]:
                # Only report error if it's not a function name or keyword
                if table_ref.upper() not in sql_keywords and table_ref.upper() not in ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN']:
                    errors.append(f"Table or alias '{table_ref}' not found in query")
        
        return errors, used_tables
    
    def _validate_performance(self, sql: str) -> List[str]:
        """Validate for performance issues."""
        warnings = []
        
        sql_upper = sql.upper()
        
        # Check against performance warning patterns
        for pattern, warning in self.PERFORMANCE_WARNINGS.items():
            if re.search(pattern, sql_upper):
                warnings.append(warning)
        
        # Check for missing semicolon
        if not sql.rstrip().endswith(';'):
            warnings.append("SQL statement should end with semicolon for clarity")
        
        # Check for Cartesian products
        from_count = sql_upper.count('FROM')
        join_count = sql_upper.count('JOIN')
        comma_in_from = re.search(r'FROM\s+\w+\s*,\s*\w+', sql_upper)
        
        if from_count > 0 and comma_in_from and join_count == 0:
            warnings.append("Possible Cartesian product detected - consider using explicit JOINs")
        
        # Check for functions in WHERE clause
        if re.search(r'WHERE.*\b(UPPER|LOWER|SUBSTR|SUBSTRING)\s*\(', sql_upper):
            warnings.append("Functions in WHERE clause may prevent index usage")
        
        return warnings
    
    def _extract_referenced_tables(self, sql: str) -> List[str]:
        """Extract all referenced table names from SQL."""
        tables = []
        
        # Remove string literals to avoid false matches
        sql_cleaned = re.sub(r"'[^']*'", ' STRING ', sql)
        sql_cleaned = re.sub(r'"[^"]*"', ' STRING ', sql_cleaned)
        
        # Remove function calls to avoid matching function names
        function_pattern = r'\b(?:COUNT|SUM|AVG|MAX|MIN|LENGTH|SUBSTR|SUBSTRING|TRIM|UPPER|LOWER|COALESCE|CAST|CONVERT|DATE|TIME|YEAR|MONTH|DAY|jsonb_extract_path|jsonb_extract_path_text|json_build_object|json_agg)\s*\([^)]*\)'
        sql_cleaned = re.sub(function_pattern, ' FUNCTION ', sql_cleaned, flags=re.IGNORECASE)
        
        # Find tables in FROM and JOIN clauses - more specific patterns
        patterns = [
            r'FROM\s+([a-zA-Z_]\w*)(?:\s+(?:AS\s+)?[a-zA-Z_]\w*)?',
            r'(?:LEFT|RIGHT|INNER|OUTER|CROSS)?\s*JOIN\s+([a-zA-Z_]\w*)',
            r'INTO\s+([a-zA-Z_]\w*)',
            r'UPDATE\s+([a-zA-Z_]\w*)',
            r'DELETE\s+FROM\s+([a-zA-Z_]\w*)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, sql_cleaned, re.IGNORECASE)
            tables.extend(matches)
        
        # Remove duplicates and common keywords that might be matched
        sql_keywords = {'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 
                       'OUTER', 'CROSS', 'ON', 'AS', 'BY', 'GROUP', 'ORDER', 'HAVING',
                       'LIMIT', 'OFFSET', 'UNION', 'ALL', 'DISTINCT', 'VALUES', 'DUAL'}
        
        tables = [t for t in set(tables) if t.upper() not in sql_keywords]
        
        return tables
    
    def _is_cte_or_subquery(self, sql: str, table_name: str) -> bool:
        """Check if a table name is defined as a CTE or subquery alias."""
        sql_upper = sql.upper()
        table_upper = table_name.upper()
        
        # Check if it's a CTE
        cte_pattern = rf'WITH\s+.*?\b{table_upper}\s+AS\s*\('
        if re.search(cte_pattern, sql_upper):
            return True
        
        # Check if it's a subquery alias
        subquery_pattern = rf'\)\s+(?:AS\s+)?{table_upper}\b'
        if re.search(subquery_pattern, sql_upper):
            return True
        
        return False
    
    def get_resource_estimate(self, sql: str) -> Dict[str, Any]:
        """Estimate resource usage for the query."""
        sql_upper = sql.upper()
        
        estimate = {
            'complexity': 'low',
            'estimated_runtime': 'fast',
            'memory_usage': 'low',
            'recommendations': []
        }
        
        # Increase complexity for various operations
        complexity_score = 0
        
        if 'JOIN' in sql_upper:
            complexity_score += sql_upper.count('JOIN')
            
        if re.search(r'GROUP\s+BY', sql_upper):
            complexity_score += 2
            
        if re.search(r'ORDER\s+BY', sql_upper):
            complexity_score += 1
            
        if 'DISTINCT' in sql_upper:
            complexity_score += 2
            
        if re.search(r'COUNT\s*\(|SUM\s*\(|AVG\s*\(', sql_upper):
            complexity_score += 1
            
        # Subqueries significantly increase complexity
        subquery_count = sql.count('(SELECT')
        complexity_score += subquery_count * 3
        
        # Determine complexity level
        if complexity_score <= 2:
            estimate['complexity'] = 'low'
            estimate['estimated_runtime'] = 'fast'
        elif complexity_score <= 5:
            estimate['complexity'] = 'medium'
            estimate['estimated_runtime'] = 'moderate'
        else:
            estimate['complexity'] = 'high'
            estimate['estimated_runtime'] = 'slow'
            estimate['memory_usage'] = 'high'
            
        # Add recommendations
        if complexity_score > 5:
            estimate['recommendations'].append("Consider breaking complex query into smaller steps")
            
        if subquery_count > 2:
            estimate['recommendations'].append("Multiple subqueries detected - consider using CTEs")
            
        return estimate