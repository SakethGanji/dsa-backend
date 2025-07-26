"""Service for data exploration, profiling, and quality analysis."""
import statistics
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
from datetime import datetime
from collections import Counter

from src.core.abstractions.service_interfaces import IExplorationService
from src.core.abstractions.repositories import ITableReader


@dataclass
class DataQualityReport:
    """Report on data quality metrics."""
    dataset_id: str
    commit_id: str
    completeness_score: float  # Percentage of non-null values
    validity_score: float      # Percentage of valid values
    uniqueness_score: float    # Percentage of unique values
    consistency_score: float   # Cross-column consistency
    overall_score: float      # Weighted average
    issues: List[Dict[str, Any]]
    generated_at: datetime


@dataclass
class DatasetProfile:
    """Comprehensive profile of a dataset."""
    dataset_id: str
    commit_id: str
    table_count: int
    total_rows: int
    total_columns: int
    data_types: Dict[str, int]  # Count by data type
    table_profiles: Dict[str, Dict[str, Any]]
    relationships: List[Dict[str, Any]]
    generated_at: datetime


@dataclass
class AnomalyReport:
    """Report on detected anomalies."""
    column_name: str
    anomaly_type: str
    severity: str  # low, medium, high
    description: str
    affected_rows: int
    examples: List[Any]


@dataclass
class Insight:
    """Data insight or pattern."""
    type: str  # correlation, trend, outlier, etc.
    description: str
    confidence: float
    details: Dict[str, Any]


class ExplorationService(IExplorationService):
    """Service for exploring and analyzing datasets."""
    
    def __init__(self, table_reader: ITableReader):
        self._table_reader = table_reader
    
    async def analyze_data_quality(
        self,
        dataset_id: str,
        commit_id: str,
        table_name: Optional[str] = None
    ) -> DataQualityReport:
        """Analyze data quality for a dataset or specific table."""
        issues = []
        scores = {
            'completeness': [],
            'validity': [],
            'uniqueness': [],
            'consistency': []
        }
        
        # Get tables to analyze
        tables = [table_name] if table_name else await self._get_all_tables(dataset_id, commit_id)
        
        for table in tables:
            # Get schema and sample data
            schema = await self._table_reader.get_table_schema(dataset_id, commit_id, table)
            sample_data = await self._get_sample_data(dataset_id, commit_id, table, limit=1000)
            
            # Analyze each column
            for col in schema.columns:
                col_name = col.name
                col_data = [row[schema.columns.index(col)] for row in sample_data]
                
                # Completeness: Check for nulls
                non_null_count = sum(1 for v in col_data if v is not None)
                completeness = non_null_count / len(col_data) if col_data else 0
                scores['completeness'].append(completeness)
                
                if completeness < 0.9:
                    issues.append({
                        'type': 'completeness',
                        'table': table,
                        'column': col_name,
                        'issue': f'High null rate: {(1-completeness)*100:.1f}%'
                    })
                
                # Validity: Check data types and constraints
                validity_score = await self._check_validity(col_data, col.data_type)
                scores['validity'].append(validity_score)
                
                # Uniqueness: Check for duplicates
                unique_count = len(set(v for v in col_data if v is not None))
                uniqueness = unique_count / non_null_count if non_null_count > 0 else 0
                scores['uniqueness'].append(uniqueness)
                
                # Check for unexpected duplicates in potential key columns
                if 'id' in col_name.lower() and uniqueness < 0.95:
                    issues.append({
                        'type': 'uniqueness',
                        'table': table,
                        'column': col_name,
                        'issue': f'Potential duplicate IDs: uniqueness {uniqueness:.1%}'
                    })
        
        # Calculate overall scores
        completeness_score = statistics.mean(scores['completeness']) if scores['completeness'] else 0
        validity_score = statistics.mean(scores['validity']) if scores['validity'] else 0
        uniqueness_score = statistics.mean(scores['uniqueness']) if scores['uniqueness'] else 0
        consistency_score = 0.95  # Placeholder - would need cross-column analysis
        
        overall_score = statistics.mean([
            completeness_score,
            validity_score,
            uniqueness_score,
            consistency_score
        ])
        
        return DataQualityReport(
            dataset_id=dataset_id,
            commit_id=commit_id,
            completeness_score=completeness_score,
            validity_score=validity_score,
            uniqueness_score=uniqueness_score,
            consistency_score=consistency_score,
            overall_score=overall_score,
            issues=issues,
            generated_at=datetime.utcnow()
        )
    
    async def profile_dataset(
        self,
        dataset_id: str,
        commit_id: str
    ) -> DatasetProfile:
        """Create comprehensive profile of a dataset."""
        tables = await self._get_all_tables(dataset_id, commit_id)
        table_profiles = {}
        data_types_count = Counter()
        total_rows = 0
        total_columns = 0
        
        for table in tables:
            # Get schema
            schema = await self._table_reader.get_table_schema(dataset_id, commit_id, table)
            
            # Get row count
            count_result = await self._table_reader.execute_query(
                dataset_id, commit_id,
                f"SELECT COUNT(*) FROM {table}"
            )
            row_count = count_result.rows[0][0] if count_result.rows else 0
            
            # Profile table
            table_profiles[table] = {
                'row_count': row_count,
                'column_count': len(schema.columns),
                'columns': [
                    {
                        'name': col.name,
                        'type': col.data_type,
                        'nullable': col.is_nullable
                    }
                    for col in schema.columns
                ],
                'primary_key': schema.primary_key,
                'indexes': schema.indexes
            }
            
            # Update totals
            total_rows += row_count
            total_columns += len(schema.columns)
            
            # Count data types
            for col in schema.columns:
                data_types_count[col.data_type] += 1
        
        # Detect relationships (simplified - look for foreign key patterns)
        relationships = self._detect_relationships(table_profiles)
        
        return DatasetProfile(
            dataset_id=dataset_id,
            commit_id=commit_id,
            table_count=len(tables),
            total_rows=total_rows,
            total_columns=total_columns,
            data_types=dict(data_types_count),
            table_profiles=table_profiles,
            relationships=relationships,
            generated_at=datetime.utcnow()
        )
    
    async def detect_anomalies(
        self,
        dataset_id: str,
        commit_id: str,
        table_name: str,
        column_name: str
    ) -> List[AnomalyReport]:
        """Detect anomalies in a specific column."""
        anomalies = []
        
        # Get column data
        query = f"SELECT {column_name} FROM {table_name}"
        result = await self._table_reader.execute_query(dataset_id, commit_id, query)
        values = [row[0] for row in result.rows if row[0] is not None]
        
        if not values:
            return anomalies
        
        # Detect based on data type
        if all(isinstance(v, (int, float)) for v in values[:100]):
            # Numerical anomalies
            anomalies.extend(self._detect_numerical_anomalies(column_name, values))
        else:
            # Categorical/text anomalies
            anomalies.extend(self._detect_categorical_anomalies(column_name, values))
        
        return anomalies
    
    async def generate_insights(
        self,
        dataset_id: str,
        commit_id: str
    ) -> List[Insight]:
        """Generate insights about the dataset."""
        insights = []
        
        # Get dataset profile
        profile = await self.profile_dataset(dataset_id, commit_id)
        
        # Table size insights
        if profile.table_count > 20:
            insights.append(Insight(
                type='complexity',
                description=f'Large schema with {profile.table_count} tables',
                confidence=1.0,
                details={'table_count': profile.table_count}
            ))
        
        # Data volume insights
        if profile.total_rows > 1_000_000:
            insights.append(Insight(
                type='volume',
                description=f'Large dataset with {profile.total_rows:,} total rows',
                confidence=1.0,
                details={'total_rows': profile.total_rows}
            ))
        
        # Data type insights
        if 'json' in profile.data_types or 'jsonb' in profile.data_types:
            insights.append(Insight(
                type='schema',
                description='Dataset contains semi-structured JSON data',
                confidence=1.0,
                details={'json_columns': profile.data_types.get('json', 0) + profile.data_types.get('jsonb', 0)}
            ))
        
        # Relationship insights
        if profile.relationships:
            insights.append(Insight(
                type='relationships',
                description=f'Found {len(profile.relationships)} potential relationships between tables',
                confidence=0.8,
                details={'relationships': profile.relationships}
            ))
        
        return insights
    
    async def _get_all_tables(self, dataset_id: str, commit_id: str) -> List[str]:
        """Get all tables in a dataset."""
        # This would typically query the dataset manifest
        # For now, return a placeholder
        return ['main_table']  # Would be implemented based on your data model
    
    async def _get_sample_data(
        self,
        dataset_id: str,
        commit_id: str,
        table_name: str,
        limit: int = 1000
    ) -> List[tuple]:
        """Get sample data from a table."""
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        result = await self._table_reader.execute_query(dataset_id, commit_id, query)
        return result.rows
    
    async def _check_validity(self, values: List[Any], data_type: str) -> float:
        """Check validity of values against expected data type."""
        if not values:
            return 1.0
        
        valid_count = 0
        total_count = len([v for v in values if v is not None])
        
        if total_count == 0:
            return 1.0
        
        for value in values:
            if value is None:
                continue
                
            try:
                if 'int' in data_type.lower():
                    int(value)
                    valid_count += 1
                elif 'float' in data_type.lower() or 'double' in data_type.lower():
                    float(value)
                    valid_count += 1
                elif 'date' in data_type.lower():
                    # Basic date validation
                    str(value)
                    valid_count += 1
                else:
                    valid_count += 1  # String types are always valid
            except:
                pass  # Invalid value
        
        return valid_count / total_count if total_count > 0 else 1.0
    
    def _detect_relationships(self, table_profiles: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Detect potential relationships between tables."""
        relationships = []
        
        # Look for foreign key patterns (column names ending with _id)
        for table1, profile1 in table_profiles.items():
            for col1 in profile1['columns']:
                if col1['name'].endswith('_id'):
                    # Look for matching table
                    potential_table = col1['name'][:-3]  # Remove _id suffix
                    if potential_table in table_profiles:
                        relationships.append({
                            'from_table': table1,
                            'from_column': col1['name'],
                            'to_table': potential_table,
                            'to_column': 'id',
                            'type': 'foreign_key'
                        })
        
        return relationships
    
    def _detect_numerical_anomalies(self, column_name: str, values: List[float]) -> List[AnomalyReport]:
        """Detect anomalies in numerical data."""
        anomalies = []
        
        if len(values) < 10:
            return anomalies
        
        # Calculate statistics
        mean_val = statistics.mean(values)
        stdev_val = statistics.stdev(values)
        
        # Detect outliers (values beyond 3 standard deviations)
        outliers = [v for v in values if abs(v - mean_val) > 3 * stdev_val]
        
        if outliers:
            anomalies.append(AnomalyReport(
                column_name=column_name,
                anomaly_type='outliers',
                severity='medium',
                description=f'Found {len(outliers)} outliers beyond 3 standard deviations',
                affected_rows=len(outliers),
                examples=outliers[:5]
            ))
        
        # Detect negative values in potentially positive-only columns
        if 'price' in column_name.lower() or 'amount' in column_name.lower():
            negative_values = [v for v in values if v < 0]
            if negative_values:
                anomalies.append(AnomalyReport(
                    column_name=column_name,
                    anomaly_type='negative_values',
                    severity='high',
                    description=f'Found {len(negative_values)} negative values in {column_name}',
                    affected_rows=len(negative_values),
                    examples=negative_values[:5]
                ))
        
        return anomalies
    
    def _detect_categorical_anomalies(self, column_name: str, values: List[str]) -> List[AnomalyReport]:
        """Detect anomalies in categorical data."""
        anomalies = []
        
        # Count frequencies
        value_counts = Counter(values)
        total_count = len(values)
        
        # Detect rare categories (less than 0.1% of data)
        rare_threshold = max(1, int(total_count * 0.001))
        rare_values = [v for v, count in value_counts.items() if count <= rare_threshold]
        
        if rare_values:
            anomalies.append(AnomalyReport(
                column_name=column_name,
                anomaly_type='rare_categories',
                severity='low',
                description=f'Found {len(rare_values)} rare categories',
                affected_rows=sum(value_counts[v] for v in rare_values),
                examples=rare_values[:5]
            ))
        
        return anomalies