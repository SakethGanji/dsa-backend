"""Text-based filter expression parser for SQL-like conditions with parentheses support."""

import re
from typing import List, Dict, Any, Tuple, Set, Optional, Union
from dataclasses import dataclass
from enum import Enum


class TokenType(Enum):
    """Token types for the expression parser."""
    LPAREN = "LPAREN"
    RPAREN = "RPAREN"
    AND = "AND"
    OR = "OR"
    IDENTIFIER = "IDENTIFIER"
    STRING = "STRING"
    NUMBER = "NUMBER"
    OPERATOR = "OPERATOR"
    COMMA = "COMMA"
    EOF = "EOF"


@dataclass
class Token:
    """Represents a token in the expression."""
    type: TokenType
    value: Any
    position: int


@dataclass
class Condition:
    """Represents a single filter condition."""
    column: str
    operator: str
    value: Any


@dataclass
class Expression:
    """Represents a parsed expression tree."""
    pass


@dataclass
class BinaryExpression(Expression):
    """Binary expression (AND/OR)."""
    left: Expression
    operator: str
    right: Expression


@dataclass
class ConditionExpression(Expression):
    """Leaf condition expression."""
    condition: Condition


class FilterExpressionParser:
    """Parser for SQL-like filter expressions with security validations."""
    
    # Whitelist of allowed operators
    ALLOWED_OPERATORS = {
        '>': '>', 
        '>=': '>=', 
        '<': '<', 
        '<=': '<=',
        '=': '=', 
        '!=': '!=', 
        '<>': '!=',
        'IN': 'IN', 
        'NOT IN': 'NOT IN',
        'LIKE': 'LIKE', 
        'ILIKE': 'ILIKE',
        'NOT LIKE': 'NOT LIKE',
        'NOT ILIKE': 'NOT ILIKE',
        'IS NULL': 'IS NULL', 
        'IS NOT NULL': 'IS NOT NULL'
    }
    
    # Token patterns
    TOKEN_PATTERNS = [
        (TokenType.LPAREN, r'\('),
        (TokenType.RPAREN, r'\)'),
        (TokenType.AND, r'\bAND\b'),
        (TokenType.OR, r'\bOR\b'),
        (TokenType.OPERATOR, r'(IS\s+NOT\s+NULL|IS\s+NULL|NOT\s+LIKE|NOT\s+ILIKE|NOT\s+IN|>=|<=|!=|<>|=|>|<|IN|LIKE|ILIKE)'),
        (TokenType.STRING, r"'([^']*)'"),
        (TokenType.NUMBER, r'-?\d+(\.\d+)?'),
        (TokenType.IDENTIFIER, r'"([^"]+)"|[a-zA-Z_][a-zA-Z0-9_]*'),  # Support "quoted identifiers" or regular identifiers
        (TokenType.COMMA, r','),
    ]
    
    def __init__(self, max_depth: int = 10, max_length: int = 1000):
        self.max_depth = max_depth
        self.max_length = max_length
        self.tokens: List[Token] = []
        self.current = 0
    
    def parse(self, expression: str) -> Expression:
        """Parse the filter expression into an AST."""
        # Basic validation
        if len(expression) > self.max_length:
            raise ValueError(f"Expression too long (max {self.max_length} characters)")
        
        # Tokenize
        self.tokens = self._tokenize(expression)
        self.current = 0
        
        # Parse with depth tracking
        return self._parse_or(depth=0)
    
    def _tokenize(self, expression: str) -> List[Token]:
        """Tokenize the expression."""
        tokens = []
        position = 0
        
        while position < len(expression):
            # Skip whitespace
            if expression[position].isspace():
                position += 1
                continue
            
            # Try to match each pattern
            matched = False
            for token_type, pattern in self.TOKEN_PATTERNS:
                regex = re.compile(pattern, re.IGNORECASE)
                match = regex.match(expression, position)
                
                if match:
                    value = match.group(0)
                    
                    # Extract string content without quotes
                    if token_type == TokenType.STRING:
                        value = match.group(1)
                    # Extract identifier content from quotes if present
                    elif token_type == TokenType.IDENTIFIER:
                        # If the identifier is quoted, extract content without quotes
                        if value.startswith('"') and value.endswith('"'):
                            value = match.group(1)
                    # Convert numbers
                    elif token_type == TokenType.NUMBER:
                        value = float(value) if '.' in value else int(value)
                    # Normalize operators
                    elif token_type == TokenType.OPERATOR:
                        value = value.upper()
                    
                    tokens.append(Token(token_type, value, position))
                    position = match.end()
                    matched = True
                    break
            
            if not matched:
                raise ValueError(f"Invalid character at position {position}: '{expression[position]}'")
        
        tokens.append(Token(TokenType.EOF, None, len(expression)))
        return tokens
    
    def _current_token(self) -> Token:
        """Get current token."""
        return self.tokens[self.current] if self.current < len(self.tokens) else self.tokens[-1]
    
    def _advance(self) -> Token:
        """Advance to next token."""
        token = self._current_token()
        if token.type != TokenType.EOF:
            self.current += 1
        return token
    
    def _parse_or(self, depth: int) -> Expression:
        """Parse OR expressions (lowest precedence)."""
        if depth > self.max_depth:
            raise ValueError(f"Expression nesting too deep (max {self.max_depth})")
        
        left = self._parse_and(depth + 1)
        
        while self._current_token().type == TokenType.OR:
            self._advance()  # consume OR
            right = self._parse_and(depth + 1)
            left = BinaryExpression(left, "OR", right)
        
        return left
    
    def _parse_and(self, depth: int) -> Expression:
        """Parse AND expressions."""
        left = self._parse_primary(depth + 1)
        
        while self._current_token().type == TokenType.AND:
            self._advance()  # consume AND
            right = self._parse_primary(depth + 1)
            left = BinaryExpression(left, "AND", right)
        
        return left
    
    def _parse_primary(self, depth: int) -> Expression:
        """Parse primary expressions (conditions or parentheses)."""
        token = self._current_token()
        
        # Handle parentheses
        if token.type == TokenType.LPAREN:
            self._advance()  # consume (
            expr = self._parse_or(depth + 1)
            if self._current_token().type != TokenType.RPAREN:
                raise ValueError(f"Expected ')' at position {self._current_token().position}")
            self._advance()  # consume )
            return expr
        
        # Parse condition
        return self._parse_condition()
    
    def _parse_condition(self) -> Expression:
        """Parse a single condition."""
        # Expect column name
        if self._current_token().type != TokenType.IDENTIFIER:
            raise ValueError(f"Expected column name at position {self._current_token().position}")
        
        column = self._advance().value
        
        # Expect operator
        if self._current_token().type != TokenType.OPERATOR:
            raise ValueError(f"Expected operator at position {self._current_token().position}")
        
        operator = self._advance().value
        
        # Handle special operators
        if operator in ['IS NULL', 'IS NOT NULL']:
            return ConditionExpression(Condition(column, operator, None))
        
        # Handle IN/NOT IN with list
        if operator in ['IN', 'NOT IN']:
            if self._current_token().type != TokenType.LPAREN:
                raise ValueError(f"Expected '(' after {operator}")
            self._advance()  # consume (
            
            values = []
            while self._current_token().type != TokenType.RPAREN:
                if self._current_token().type not in [TokenType.STRING, TokenType.NUMBER]:
                    raise ValueError(f"Expected value in {operator} list")
                values.append(self._advance().value)
                
                if self._current_token().type == TokenType.COMMA:
                    self._advance()  # consume comma
                elif self._current_token().type != TokenType.RPAREN:
                    raise ValueError(f"Expected ',' or ')' in {operator} list")
            
            self._advance()  # consume )
            
            # Validate that IN list is not empty
            if not values:
                raise ValueError(f"{operator} list cannot be empty")
                
            return ConditionExpression(Condition(column, operator, values))
        
        # Regular comparison operators
        if self._current_token().type not in [TokenType.STRING, TokenType.NUMBER]:
            raise ValueError(f"Expected value at position {self._current_token().position}")
        
        value = self._advance().value
        return ConditionExpression(Condition(column, operator, value))
    
    def to_sql(
        self, 
        expr: Expression, 
        valid_columns: Set[str],
        column_types: Dict[str, str],
        param_start: int = 1
    ) -> Tuple[str, List[Any]]:
        """Convert expression to parameterized SQL with column validation."""
        params = []
        sql = self._expr_to_sql(expr, valid_columns, column_types, params, param_start)
        return sql, params
    
    def _expr_to_sql(
        self,
        expr: Expression,
        valid_columns: Set[str],
        column_types: Dict[str, str],
        params: List[Any],
        param_offset: int
    ) -> str:
        """Recursively convert expression to SQL."""
        if isinstance(expr, BinaryExpression):
            left = self._expr_to_sql(expr.left, valid_columns, column_types, params, param_offset)
            right = self._expr_to_sql(expr.right, valid_columns, column_types, params, param_offset)
            return f"({left} {expr.operator} {right})"
        
        elif isinstance(expr, ConditionExpression):
            cond = expr.condition
            
            # Validate column
            if cond.column not in valid_columns:
                raise ValueError(f"Invalid column: {cond.column}")
            
            # Validate operator
            if cond.operator not in self.ALLOWED_OPERATORS:
                raise ValueError(f"Invalid operator: {cond.operator}")
            
            sql_op = self.ALLOWED_OPERATORS[cond.operator]
            col_type = column_types.get(cond.column, 'text')
            type_cast = self._get_type_cast(col_type)
            
            # Build SQL based on operator
            # Handle nested data structure (data might be under 'data' key)
            data_extract = """(CASE 
                             WHEN r.data ? 'data' THEN r.data->'data'->>'{0}'
                             ELSE r.data->>'{0}'
                             END)""".format(cond.column)
            
            if cond.operator in ['IS NULL', 'IS NOT NULL']:
                return f"{data_extract} {sql_op}"
            
            elif cond.operator in ['IN', 'NOT IN']:
                placeholders = []
                for val in cond.value:
                    params.append(val)
                    placeholders.append(f"${len(params) + param_offset - 1}")
                return f"{data_extract}{type_cast} {sql_op} ({', '.join(placeholders)})"
            
            else:
                params.append(cond.value)
                param_num = len(params) + param_offset - 1
                return f"{data_extract}{type_cast} {sql_op} ${param_num}"
        
        else:
            raise ValueError(f"Unknown expression type: {type(expr)}")
    
    def _get_type_cast(self, col_type: str) -> str:
        """Get PostgreSQL type cast for column type."""
        type_map = {
            'integer': '::integer',
            'bigint': '::bigint',
            'numeric': '::numeric',
            'float': '::float',
            'double': '::double precision',
            'boolean': '::boolean',
            'date': '::date',
            'timestamp': '::timestamp',
            'time': '::time',
            'text': '',
            'string': '',
            'varchar': ''
        }
        return type_map.get(col_type.lower(), '')