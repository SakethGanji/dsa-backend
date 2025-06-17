from typing import Any, Dict, List, Optional, Tuple, Union

from sqlalchemy import and_, or_, select
from sqlalchemy.sql import Select


class QueryBuilder:
    """Helper class for building SQL queries."""
    
    def __init__(self, base_query: Optional[Select] = None):
        self.query = base_query
        self._params: Dict[str, Any] = {}
    
    def select(self, *columns) -> 'QueryBuilder':
        """Start a SELECT query."""
        self.query = select(*columns)
        return self
    
    def from_table(self, table) -> 'QueryBuilder':
        """Add FROM clause."""
        if self.query is None:
            self.query = select(table)
        else:
            self.query = self.query.select_from(table)
        return self
    
    def where(self, *conditions) -> 'QueryBuilder':
        """Add WHERE conditions."""
        if conditions:
            self.query = self.query.where(and_(*conditions))
        return self
    
    def or_where(self, *conditions) -> 'QueryBuilder':
        """Add OR WHERE conditions."""
        if conditions:
            self.query = self.query.where(or_(*conditions))
        return self
    
    def join(self, table, condition) -> 'QueryBuilder':
        """Add JOIN clause."""
        self.query = self.query.join(table, condition)
        return self
    
    def left_join(self, table, condition) -> 'QueryBuilder':
        """Add LEFT JOIN clause."""
        self.query = self.query.outerjoin(table, condition)
        return self
    
    def group_by(self, *columns) -> 'QueryBuilder':
        """Add GROUP BY clause."""
        self.query = self.query.group_by(*columns)
        return self
    
    def having(self, condition) -> 'QueryBuilder':
        """Add HAVING clause."""
        self.query = self.query.having(condition)
        return self
    
    def order_by(self, *columns) -> 'QueryBuilder':
        """Add ORDER BY clause."""
        self.query = self.query.order_by(*columns)
        return self
    
    def limit(self, limit: int) -> 'QueryBuilder':
        """Add LIMIT clause."""
        self.query = self.query.limit(limit)
        return self
    
    def offset(self, offset: int) -> 'QueryBuilder':
        """Add OFFSET clause."""
        self.query = self.query.offset(offset)
        return self
    
    def paginate(self, page: int, per_page: int) -> 'QueryBuilder':
        """Add pagination."""
        offset = (page - 1) * per_page
        return self.limit(per_page).offset(offset)
    
    def build(self) -> Select:
        """Build and return the query."""
        return self.query
    
    def add_param(self, key: str, value: Any) -> 'QueryBuilder':
        """Add a parameter for parameterized queries."""
        self._params[key] = value
        return self
    
    @property
    def params(self) -> Dict[str, Any]:
        """Get query parameters."""
        return self._params


class RawQueryBuilder:
    """Helper for building raw SQL queries safely."""
    
    def __init__(self):
        self.query_parts: List[str] = []
        self.params: Dict[str, Any] = {}
        self._param_counter = 0
    
    def append(self, sql: str) -> 'RawQueryBuilder':
        """Append SQL fragment."""
        self.query_parts.append(sql)
        return self
    
    def append_param(self, value: Any) -> str:
        """Add a parameter and return its placeholder."""
        param_name = f"param_{self._param_counter}"
        self._param_counter += 1
        self.params[param_name] = value
        return f":{param_name}"
    
    def append_with_params(self, sql: str, **params) -> 'RawQueryBuilder':
        """Append SQL with named parameters."""
        self.query_parts.append(sql)
        self.params.update(params)
        return self
    
    def build(self) -> Tuple[str, Dict[str, Any]]:
        """Build the query and return (query_string, params)."""
        query = " ".join(self.query_parts)
        return query, self.params


def build_insert_query(table_name: str, data: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """Build an INSERT query."""
    columns = list(data.keys())
    values = [f":{col}" for col in columns]
    
    query = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({', '.join(values)})
        RETURNING id
    """
    
    return query.strip(), data


def build_update_query(
    table_name: str,
    data: Dict[str, Any],
    where_conditions: Dict[str, Any]
) -> Tuple[str, Dict[str, Any]]:
    """Build an UPDATE query."""
    set_clauses = [f"{col} = :set_{col}" for col in data.keys()]
    where_clauses = [f"{col} = :where_{col}" for col in where_conditions.keys()]
    
    query = f"""
        UPDATE {table_name}
        SET {', '.join(set_clauses)}
        WHERE {' AND '.join(where_clauses)}
    """
    
    params = {}
    for key, value in data.items():
        params[f"set_{key}"] = value
    for key, value in where_conditions.items():
        params[f"where_{key}"] = value
    
    return query.strip(), params


def build_delete_query(
    table_name: str,
    where_conditions: Dict[str, Any]
) -> Tuple[str, Dict[str, Any]]:
    """Build a DELETE query."""
    where_clauses = [f"{col} = :{col}" for col in where_conditions.keys()]
    
    query = f"""
        DELETE FROM {table_name}
        WHERE {' AND '.join(where_clauses)}
    """
    
    return query.strip(), where_conditions