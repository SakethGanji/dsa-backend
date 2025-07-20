"""Implementation of table analysis services."""

from typing import Dict, Any, List, Optional
import statistics
from collections import Counter
from datetime import datetime
import re

from src.core.abstractions.services import (
    ITableAnalysisService,
    IDataTypeInferenceService, 
    IColumnStatisticsService,
    TableAnalysis,
    ColumnStatistics as ServiceColumnStatistics,
    TableSchema
)
from src.core.abstractions import ITableReader
from src.core.domain_exceptions import ValidationException


class DataTypeInferenceService(IDataTypeInferenceService):
    """Service for inferring data types from sample values."""
    
    def __init__(self):
        # Define regex patterns for type detection
        self.patterns = {
            'integer': re.compile(r'^-?\d+$'),
            'float': re.compile(r'^-?\d*\.\d+$'),
            'boolean': re.compile(r'^(true|false|yes|no|1|0)$', re.IGNORECASE),
            'date': re.compile(r'^\d{4}-\d{2}-\d{2}$'),
            'datetime': re.compile(r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}'),
            'email': re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
            'url': re.compile(r'^https?://'),
            'uuid': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
        }
    
    def infer_column_type(self, values: List[Any]) -> str:
        """Infer the data type of a column based on sample values."""
        if not values:
            return 'string'
        
        # Remove None values
        non_null_values = [v for v in values if v is not None]
        if not non_null_values:
            return 'string'
        
        # Count type matches
        type_counts = Counter()
        
        for value in non_null_values[:100]:  # Sample first 100 values
            value_str = str(value)
            
            # Check each pattern
            if self.patterns['integer'].match(value_str):
                type_counts['integer'] += 1
            elif self.patterns['float'].match(value_str):
                type_counts['float'] += 1
            elif self.patterns['boolean'].match(value_str):
                type_counts['boolean'] += 1
            elif self.patterns['datetime'].match(value_str):
                type_counts['datetime'] += 1
            elif self.patterns['date'].match(value_str):
                type_counts['date'] += 1
            elif self.patterns['email'].match(value_str):
                type_counts['email'] += 1
            elif self.patterns['url'].match(value_str):
                type_counts['url'] += 1
            elif self.patterns['uuid'].match(value_str):
                type_counts['uuid'] += 1
            else:
                type_counts['string'] += 1
        
        # Return most common type
        if type_counts:
            return type_counts.most_common(1)[0][0]
        return 'string'
    
    def validate_type_consistency(
        self,
        values: List[Any],
        expected_type: str
    ) -> Dict[str, Any]:
        """Validate that values are consistent with expected type."""
        inconsistent_indices = []
        inconsistent_values = []
        
        pattern = self.patterns.get(expected_type)
        if not pattern:
            return {
                'is_consistent': True,
                'inconsistent_count': 0,
                'inconsistent_indices': [],
                'inconsistent_values': []
            }
        
        for idx, value in enumerate(values):
            if value is not None:
                if not pattern.match(str(value)):
                    inconsistent_indices.append(idx)
                    inconsistent_values.append(value)
        
        return {
            'is_consistent': len(inconsistent_indices) == 0,
            'inconsistent_count': len(inconsistent_indices),
            'inconsistent_indices': inconsistent_indices[:10],  # First 10
            'inconsistent_values': inconsistent_values[:10]
        }
    
    def get_type_hierarchy(self) -> Dict[str, List[str]]:
        """Get the type hierarchy for type coercion."""
        return {
            'integer': ['float', 'string'],
            'float': ['string'],
            'boolean': ['integer', 'string'],
            'date': ['datetime', 'string'],
            'datetime': ['string'],
            'email': ['string'],
            'url': ['string'],
            'uuid': ['string'],
            'string': []
        }


class ColumnStatisticsService(IColumnStatisticsService):
    """Service for computing column statistics."""
    
    async def compute_numeric_statistics(
        self,
        values: List[float]
    ) -> Dict[str, float]:
        """Compute statistics for numeric columns."""
        if not values:
            return {}
        
        clean_values = [v for v in values if v is not None]
        if not clean_values:
            return {}
        
        sorted_values = sorted(clean_values)
        n = len(sorted_values)
        
        return {
            'min': min(clean_values),
            'max': max(clean_values),
            'mean': statistics.mean(clean_values),
            'median': statistics.median(clean_values),
            'std_dev': statistics.stdev(clean_values) if n > 1 else 0.0,
            'variance': statistics.variance(clean_values) if n > 1 else 0.0,
            'q1': sorted_values[n // 4] if n >= 4 else sorted_values[0],
            'q3': sorted_values[3 * n // 4] if n >= 4 else sorted_values[-1],
            'iqr': sorted_values[3 * n // 4] - sorted_values[n // 4] if n >= 4 else 0.0,
            'skewness': self._compute_skewness(clean_values),
            'kurtosis': self._compute_kurtosis(clean_values)
        }
    
    async def compute_string_statistics(
        self,
        values: List[str]
    ) -> Dict[str, Any]:
        """Compute statistics for string columns."""
        if not values:
            return {}
        
        clean_values = [v for v in values if v is not None]
        if not clean_values:
            return {}
        
        lengths = [len(str(v)) for v in clean_values]
        value_counts = Counter(clean_values)
        
        return {
            'min_length': min(lengths),
            'max_length': max(lengths),
            'avg_length': statistics.mean(lengths),
            'most_common': value_counts.most_common(10),
            'unique_count': len(value_counts),
            'has_leading_spaces': any(str(v).startswith(' ') for v in clean_values),
            'has_trailing_spaces': any(str(v).endswith(' ') for v in clean_values),
            'has_mixed_case': any(v != v.lower() and v != v.upper() for v in clean_values if isinstance(v, str))
        }
    
    async def compute_date_statistics(
        self,
        values: List[Any]
    ) -> Dict[str, Any]:
        """Compute statistics for date/time columns."""
        if not values:
            return {}
        
        # Convert to datetime objects
        dates = []
        for v in values:
            if v is not None:
                try:
                    if isinstance(v, str):
                        dates.append(datetime.fromisoformat(v.replace('Z', '+00:00')))
                    elif isinstance(v, datetime):
                        dates.append(v)
                except:
                    pass
        
        if not dates:
            return {}
        
        sorted_dates = sorted(dates)
        
        return {
            'min_date': sorted_dates[0].isoformat(),
            'max_date': sorted_dates[-1].isoformat(),
            'date_range_days': (sorted_dates[-1] - sorted_dates[0]).days,
            'unique_dates': len(set(d.date() for d in dates)),
            'weekend_count': sum(1 for d in dates if d.weekday() >= 5),
            'weekday_distribution': Counter(d.strftime('%A') for d in dates).most_common()
        }
    
    async def detect_outliers(
        self,
        values: List[float],
        method: str = "iqr"
    ) -> List[int]:
        """Detect outlier indices in numeric data."""
        if not values or len(values) < 4:
            return []
        
        clean_values = [(i, v) for i, v in enumerate(values) if v is not None]
        if len(clean_values) < 4:
            return []
        
        if method == "iqr":
            sorted_values = sorted(clean_values, key=lambda x: x[1])
            n = len(sorted_values)
            q1 = sorted_values[n // 4][1]
            q3 = sorted_values[3 * n // 4][1]
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            return [i for i, v in clean_values if v < lower_bound or v > upper_bound]
        
        elif method == "zscore":
            mean = statistics.mean([v for _, v in clean_values])
            std = statistics.stdev([v for _, v in clean_values])
            if std == 0:
                return []
            
            return [i for i, v in clean_values if abs((v - mean) / std) > 3]
        
        else:
            raise ValueError(f"Unknown outlier detection method: {method}")
    
    async def compute_correlations(
        self,
        columns: Dict[str, List[float]]
    ) -> Dict[str, Dict[str, float]]:
        """Compute correlations between numeric columns."""
        correlations = {}
        
        for col1, values1 in columns.items():
            correlations[col1] = {}
            for col2, values2 in columns.items():
                if len(values1) == len(values2):
                    corr = self._pearson_correlation(values1, values2)
                    correlations[col1][col2] = corr
        
        return correlations
    
    def _compute_skewness(self, values: List[float]) -> float:
        """Compute skewness of numeric values."""
        if len(values) < 3:
            return 0.0
        
        mean = statistics.mean(values)
        std = statistics.stdev(values)
        if std == 0:
            return 0.0
        
        n = len(values)
        skew = sum(((x - mean) / std) ** 3 for x in values) * n / ((n - 1) * (n - 2))
        return skew
    
    def _compute_kurtosis(self, values: List[float]) -> float:
        """Compute kurtosis of numeric values."""
        if len(values) < 4:
            return 0.0
        
        mean = statistics.mean(values)
        std = statistics.stdev(values)
        if std == 0:
            return 0.0
        
        n = len(values)
        kurt = sum(((x - mean) / std) ** 4 for x in values) * n * (n + 1) / ((n - 1) * (n - 2) * (n - 3))
        kurt -= 3 * (n - 1) ** 2 / ((n - 2) * (n - 3))
        return kurt
    
    def _pearson_correlation(self, x: List[float], y: List[float]) -> float:
        """Compute Pearson correlation coefficient."""
        if len(x) != len(y) or len(x) < 2:
            return 0.0
        
        # Remove pairs with None values
        pairs = [(xi, yi) for xi, yi in zip(x, y) if xi is not None and yi is not None]
        if len(pairs) < 2:
            return 0.0
        
        x_clean = [p[0] for p in pairs]
        y_clean = [p[1] for p in pairs]
        
        x_mean = statistics.mean(x_clean)
        y_mean = statistics.mean(y_clean)
        
        numerator = sum((xi - x_mean) * (yi - y_mean) for xi, yi in pairs)
        x_std = statistics.stdev(x_clean)
        y_std = statistics.stdev(y_clean)
        
        if x_std == 0 or y_std == 0:
            return 0.0
        
        denominator = x_std * y_std * (len(pairs) - 1)
        return numerator / denominator if denominator != 0 else 0.0


class TableAnalysisService(ITableAnalysisService):
    """Service for comprehensive table analysis."""
    
    def __init__(
        self,
        table_reader: ITableReader,
        type_inference_service: IDataTypeInferenceService,
        statistics_service: IColumnStatisticsService
    ):
        self._table_reader = table_reader
        self._type_inference = type_inference_service
        self._statistics = statistics_service
    
    async def analyze_table(
        self,
        commit_id: str,
        table_key: str,
        sample_size: int = 100,
        compute_statistics: bool = True,
        infer_types: bool = True
    ) -> TableAnalysis:
        """Perform comprehensive analysis on a table."""
        # Get schema
        schema_data = await self._table_reader.get_table_schema(commit_id, table_key)
        if not schema_data:
            schema_data = {}
        
        schema = TableSchema(
            columns=schema_data.get('columns', []),
            primary_key=schema_data.get('primary_key'),
            row_count=schema_data.get('row_count', 0),
            size_bytes=schema_data.get('size_bytes')
        )
        
        # Get sample data
        sample_data = await self._table_reader.get_table_data(
            commit_id, table_key, limit=sample_size
        )
        
        # Analyze columns
        column_stats = []
        sample_values = {}
        quality_issues = []
        
        if sample_data:
            # Convert rows to columnar format
            columns = {}
            for row in sample_data:
                for col, value in row.items():
                    if col not in columns:
                        columns[col] = []
                    columns[col].append(value)
            
            # Analyze each column
            for col_name, values in columns.items():
                # Infer type
                inferred_type = self._type_inference.infer_column_type(values) if infer_types else 'string'
                
                # Compute statistics
                stats_dict = {}
                if compute_statistics:
                    if inferred_type in ['integer', 'float']:
                        numeric_values = []
                        for v in values:
                            try:
                                numeric_values.append(float(v) if v is not None else None)
                            except:
                                pass
                        stats_dict = await self._statistics.compute_numeric_statistics(
                            [v for v in numeric_values if v is not None]
                        )
                    elif inferred_type == 'string':
                        stats_dict = await self._statistics.compute_string_statistics(values)
                    elif inferred_type in ['date', 'datetime']:
                        stats_dict = await self._statistics.compute_date_statistics(values)
                
                # Create column statistics
                null_count = sum(1 for v in values if v is None)
                non_null_count = len(values) - null_count
                unique_values = set(v for v in values if v is not None)
                
                col_stats = ServiceColumnStatistics(
                    column_name=col_name,
                    data_type=inferred_type,
                    non_null_count=non_null_count,
                    null_count=null_count,
                    unique_count=len(unique_values),
                    min_value=stats_dict.get('min'),
                    max_value=stats_dict.get('max'),
                    mean_value=stats_dict.get('mean'),
                    median_value=stats_dict.get('median'),
                    std_dev=stats_dict.get('std_dev'),
                    percentiles={
                        '25': stats_dict.get('q1'),
                        '75': stats_dict.get('q3')
                    } if 'q1' in stats_dict else None
                )
                column_stats.append(col_stats)
                
                # Sample values
                sample_values[col_name] = list(unique_values)[:10]
                
                # Check for quality issues
                if null_count > non_null_count:
                    quality_issues.append({
                        'type': 'high_nulls',
                        'column': col_name,
                        'severity': 'high',
                        'details': f'{null_count} nulls out of {len(values)} values'
                    })
                
                if inferred_type in ['integer', 'float'] and 'outliers' in stats_dict:
                    outlier_indices = await self._statistics.detect_outliers(
                        [float(v) if v is not None else None for v in values]
                    )
                    if outlier_indices:
                        quality_issues.append({
                            'type': 'outliers',
                            'column': col_name,
                            'severity': 'medium',
                            'details': f'{len(outlier_indices)} potential outliers detected'
                        })
        
        # Create profiling metadata
        profiling_metadata = {
            'analysis_timestamp': datetime.utcnow().isoformat(),
            'sample_size': sample_size,
            'compute_statistics': compute_statistics,
            'infer_types': infer_types,
            'total_columns': len(column_stats),
            'quality_score': self._calculate_quality_score(quality_issues)
        }
        
        return TableAnalysis(
            schema=schema,
            statistics=column_stats,
            sample_values=sample_values,
            data_quality_issues=quality_issues,
            profiling_metadata=profiling_metadata
        )
    
    async def get_column_profile(
        self,
        commit_id: str,
        table_key: str,
        column_name: str
    ) -> ServiceColumnStatistics:
        """Get detailed profile for a single column."""
        # Get column data
        sample_data = await self._table_reader.get_table_data(
            commit_id, table_key, columns=[column_name], limit=10000
        )
        
        if not sample_data:
            raise ValidationException(f"Column {column_name} not found", field="column_name")
        
        values = [row.get(column_name) for row in sample_data]
        
        # Infer type
        inferred_type = self._type_inference.infer_column_type(values)
        
        # Compute statistics
        stats_dict = {}
        if inferred_type in ['integer', 'float']:
            numeric_values = []
            for v in values:
                try:
                    numeric_values.append(float(v) if v is not None else None)
                except:
                    pass
            stats_dict = await self._statistics.compute_numeric_statistics(
                [v for v in numeric_values if v is not None]
            )
        elif inferred_type == 'string':
            stats_dict = await self._statistics.compute_string_statistics(values)
        elif inferred_type in ['date', 'datetime']:
            stats_dict = await self._statistics.compute_date_statistics(values)
        
        # Create column statistics
        null_count = sum(1 for v in values if v is None)
        non_null_count = len(values) - null_count
        unique_values = set(v for v in values if v is not None)
        
        return ServiceColumnStatistics(
            column_name=column_name,
            data_type=inferred_type,
            non_null_count=non_null_count,
            null_count=null_count,
            unique_count=len(unique_values),
            min_value=stats_dict.get('min'),
            max_value=stats_dict.get('max'),
            mean_value=stats_dict.get('mean'),
            median_value=stats_dict.get('median'),
            std_dev=stats_dict.get('std_dev'),
            percentiles={
                '25': stats_dict.get('q1'),
                '75': stats_dict.get('q3')
            } if 'q1' in stats_dict else None
        )
    
    def _calculate_quality_score(self, issues: List[Dict[str, Any]]) -> float:
        """Calculate a quality score based on issues found."""
        if not issues:
            return 100.0
        
        severity_weights = {
            'high': 10,
            'medium': 5,
            'low': 2
        }
        
        total_penalty = sum(
            severity_weights.get(issue.get('severity', 'low'), 2)
            for issue in issues
        )
        
        # Score decreases with more/severe issues
        score = max(0, 100 - total_penalty)
        return score