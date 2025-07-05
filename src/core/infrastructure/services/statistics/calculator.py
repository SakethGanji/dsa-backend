"""Default implementation of statistics calculator."""

from typing import Dict, Any
import pandas as pd
import numpy as np

from ....abstractions.services import (
    IStatisticsService,
    TableStatistics,
    ColumnStatistics
)


class DefaultStatisticsCalculator(IStatisticsService):
    """Default implementation for calculating table statistics."""
    
    async def calculate_table_statistics(
        self,
        dataframe: pd.DataFrame,
        table_key: str
    ) -> TableStatistics:
        """Calculate comprehensive statistics for a table."""
        # Calculate column statistics
        columns = {}
        for col_name in dataframe.columns:
            col_stats = await self.calculate_column_statistics(
                dataframe[col_name], col_name
            )
            columns[col_name] = col_stats
        
        # Calculate memory usage
        memory_usage_bytes = dataframe.memory_usage(deep=True).sum()
        
        # Calculate unique/duplicate rows
        unique_row_count = len(dataframe.drop_duplicates())
        duplicate_row_count = len(dataframe) - unique_row_count
        
        return TableStatistics(
            row_count=len(dataframe),
            column_count=len(dataframe.columns),
            columns=columns,
            memory_usage_bytes=memory_usage_bytes,
            unique_row_count=unique_row_count,
            duplicate_row_count=duplicate_row_count
        )
    
    async def calculate_column_statistics(
        self,
        series: pd.Series,
        column_name: str
    ) -> ColumnStatistics:
        """Calculate statistics for a single column."""
        # Basic stats
        dtype = str(series.dtype)
        null_count = series.isnull().sum()
        null_percentage = (null_count / len(series) * 100) if len(series) > 0 else 0
        unique_count = series.nunique()
        
        # Initialize optional stats
        min_value = None
        max_value = None
        mean_value = None
        median_value = None
        std_dev = None
        
        # Calculate numeric statistics if applicable
        if pd.api.types.is_numeric_dtype(series):
            non_null_series = series.dropna()
            if len(non_null_series) > 0:
                min_value = float(non_null_series.min())
                max_value = float(non_null_series.max())
                mean_value = float(non_null_series.mean())
                median_value = float(non_null_series.median())
                if len(non_null_series) > 1:
                    std_dev = float(non_null_series.std())
        
        # For datetime columns
        elif pd.api.types.is_datetime64_any_dtype(series):
            non_null_series = series.dropna()
            if len(non_null_series) > 0:
                min_value = non_null_series.min().isoformat()
                max_value = non_null_series.max().isoformat()
        
        # For string/object columns, get min/max by length or alphabetically
        else:
            non_null_series = series.dropna()
            if len(non_null_series) > 0:
                str_series = non_null_series.astype(str)
                min_value = str_series.min()
                max_value = str_series.max()
        
        return ColumnStatistics(
            name=column_name,
            dtype=self._map_dtype_to_type(dtype),
            null_count=int(null_count),
            null_percentage=float(null_percentage),
            unique_count=int(unique_count),
            min_value=min_value,
            max_value=max_value,
            mean_value=mean_value,
            median_value=median_value,
            std_dev=std_dev
        )
    
    def get_summary_dict(self, stats: TableStatistics) -> Dict[str, Any]:
        """Convert TableStatistics to a dictionary suitable for storage."""
        return {
            'row_count': stats.row_count,
            'column_count': stats.column_count,
            'memory_usage_bytes': stats.memory_usage_bytes,
            'unique_row_count': stats.unique_row_count,
            'duplicate_row_count': stats.duplicate_row_count,
            'columns': {
                col_name: {
                    'dtype': col_stats.dtype,
                    'null_count': col_stats.null_count,
                    'null_percentage': col_stats.null_percentage,
                    'unique_count': col_stats.unique_count,
                    'min_value': col_stats.min_value,
                    'max_value': col_stats.max_value,
                    'mean_value': col_stats.mean_value,
                    'median_value': col_stats.median_value,
                    'std_dev': col_stats.std_dev
                }
                for col_name, col_stats in stats.columns.items()
            }
        }
    
    def _map_dtype_to_type(self, dtype: str) -> str:
        """Map pandas dtype to our type system."""
        if 'int' in dtype:
            return 'integer'
        elif 'float' in dtype:
            return 'number'
        elif 'bool' in dtype:
            return 'boolean'
        elif 'datetime' in dtype:
            return 'datetime'
        elif 'timedelta' in dtype:
            return 'timedelta'
        elif 'category' in dtype:
            return 'category'
        else:
            return 'string'