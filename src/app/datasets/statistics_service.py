"""Service for calculating dataset statistics from Parquet files"""
import os
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from collections import Counter

logger = logging.getLogger(__name__)


class StatisticsService:
    """Service for calculating statistics from Parquet files using metadata and optional data scanning"""
    
    @staticmethod
    async def calculate_parquet_statistics(file_path: str, detailed: bool = False, sample_size: Optional[int] = None) -> Dict[str, Any]:
        """
        Calculate statistics from Parquet file
        
        Args:
            file_path: Path to the Parquet file
            detailed: Whether to calculate detailed statistics (requires data scanning)
            sample_size: Number of rows to sample for detailed statistics
            
        Returns:
            Dictionary containing statistics
        """
        start_time = time.time()
        
        try:
            # Read Parquet metadata
            parquet_file = pq.ParquetFile(file_path)
            metadata = parquet_file.metadata
            schema = parquet_file.schema_arrow
            
            # High-level stats
            row_count = metadata.num_rows
            column_count = len(schema)
            size_bytes = os.path.getsize(file_path)
            
            # Column-level stats from Parquet metadata
            column_stats = StatisticsService._extract_metadata_statistics(parquet_file, metadata)
            
            # If detailed statistics requested, scan the data
            if detailed:
                detailed_stats = await StatisticsService._calculate_detailed_statistics(
                    file_path, column_stats, sample_size
                )
                # Merge detailed stats with metadata stats
                for col_name, stats in detailed_stats.items():
                    if col_name in column_stats:
                        column_stats[col_name].update(stats)
            
            # Calculate null percentages
            for col_name, stats in column_stats.items():
                if "null_count" in stats and row_count > 0:
                    stats["null_percentage"] = round((stats["null_count"] / row_count) * 100, 2)
                else:
                    stats["null_percentage"] = 0.0
            
            duration_ms = int((time.time() - start_time) * 1000)
            
            return {
                "row_count": row_count,
                "column_count": column_count,
                "size_bytes": size_bytes,
                "statistics": {
                    "columns": column_stats,
                    "metadata": {
                        "profiling_method": "parquet_metadata" if not detailed else "detailed_scan",
                        "sampling_applied": sample_size is not None and sample_size < row_count,
                        "sample_size": sample_size if sample_size else row_count,
                        "profiling_duration_ms": duration_ms
                    }
                }
            }
            
        except Exception as e:
            logger.error(f"Error calculating statistics for {file_path}: {str(e)}")
            raise
    
    @staticmethod
    def _extract_metadata_statistics(parquet_file: pq.ParquetFile, metadata: Any) -> Dict[str, Dict[str, Any]]:
        """Extract statistics from Parquet metadata without reading data"""
        column_stats = {}
        schema = parquet_file.schema_arrow
        
        # Initialize stats for each column
        for field in schema:
            column_stats[field.name] = {
                "data_type": str(field.type),
                "nullable": field.nullable,
                "null_count": 0,
                "min_value": None,
                "max_value": None
            }
        
        # Aggregate statistics from all row groups
        for i in range(metadata.num_row_groups):
            row_group = metadata.row_group(i)
            
            for j in range(row_group.num_columns):
                col_meta = row_group.column(j)
                col_name = parquet_file.schema.names[j]
                
                if col_meta.statistics and col_meta.statistics.has_min_max:
                    stats = col_meta.statistics
                    
                    # Accumulate null counts
                    column_stats[col_name]["null_count"] += stats.null_count
                    
                    # Update min/max values
                    current_min = column_stats[col_name]["min_value"]
                    current_max = column_stats[col_name]["max_value"]
                    
                    try:
                        # Convert min/max to Python types
                        min_val = StatisticsService._convert_arrow_value(stats.min)
                        max_val = StatisticsService._convert_arrow_value(stats.max)
                        
                        if current_min is None or (min_val is not None and min_val < current_min):
                            column_stats[col_name]["min_value"] = min_val
                        
                        if current_max is None or (max_val is not None and max_val > current_max):
                            column_stats[col_name]["max_value"] = max_val
                    except Exception as e:
                        logger.debug(f"Could not extract min/max for column {col_name}: {e}")
        
        return column_stats
    
    @staticmethod
    def _convert_arrow_value(value: Any) -> Any:
        """Convert Arrow/Parquet value to Python type"""
        if value is None:
            return None
        
        # Handle common types
        if hasattr(value, 'as_py'):
            return value.as_py()
        
        # Handle date/timestamp types
        if hasattr(value, 'isoformat'):
            return value.isoformat()
        
        # Handle bytes
        if isinstance(value, bytes):
            try:
                return value.decode('utf-8')
            except:
                return str(value)
        
        return value
    
    @staticmethod
    async def _calculate_detailed_statistics(
        file_path: str, 
        column_stats: Dict[str, Dict[str, Any]], 
        sample_size: Optional[int] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Calculate detailed statistics by scanning the data"""
        # Read the Parquet file
        df = pd.read_parquet(file_path)
        
        # Apply sampling if needed
        if sample_size and len(df) > sample_size:
            df = df.sample(n=sample_size, random_state=42)
            logger.info(f"Sampled {sample_size} rows from {len(df)} total rows for detailed statistics")
        
        detailed_stats = {}
        
        for column in df.columns:
            col_data = df[column]
            col_stats = {}
            
            # Distinct count
            try:
                col_stats["distinct_count"] = int(col_data.nunique())
            except:
                col_stats["distinct_count"] = None
            
            # Type-specific statistics
            if pd.api.types.is_numeric_dtype(col_data):
                # Numeric columns: calculate histogram
                non_null_data = col_data.dropna()
                if len(non_null_data) > 0:
                    try:
                        # Calculate histogram with 20 bins
                        hist, bin_edges = np.histogram(non_null_data, bins=20)
                        col_stats["histogram"] = {
                            "bins": [float(x) for x in bin_edges.tolist()],
                            "counts": [int(x) for x in hist.tolist()]
                        }
                        
                        # Additional numeric stats
                        col_stats["mean"] = float(non_null_data.mean())
                        col_stats["median"] = float(non_null_data.median())
                        col_stats["std_dev"] = float(non_null_data.std())
                        
                        # Quartiles
                        col_stats["percentiles"] = {
                            "25": float(non_null_data.quantile(0.25)),
                            "50": float(non_null_data.quantile(0.50)),
                            "75": float(non_null_data.quantile(0.75))
                        }
                    except Exception as e:
                        logger.debug(f"Could not calculate numeric statistics for column {column}: {e}")
            
            elif pd.api.types.is_string_dtype(col_data) or pd.api.types.is_categorical_dtype(col_data):
                # String/categorical columns: top values
                try:
                    value_counts = col_data.value_counts()
                    top_n = min(10, len(value_counts))
                    
                    if top_n > 0:
                        top_values = value_counts.head(top_n)
                        col_stats["top_values"] = [
                            {"value": str(val), "count": int(count)} 
                            for val, count in top_values.items()
                        ]
                        
                        # Add percentage for top values
                        total_non_null = len(col_data.dropna())
                        if total_non_null > 0:
                            for item in col_stats["top_values"]:
                                item["percentage"] = round((item["count"] / total_non_null) * 100, 2)
                except Exception as e:
                    logger.debug(f"Could not calculate top values for column {column}: {e}")
            
            elif pd.api.types.is_datetime64_any_dtype(col_data):
                # Date/time columns: date range statistics
                try:
                    non_null_data = col_data.dropna()
                    if len(non_null_data) > 0:
                        col_stats["date_range"] = {
                            "min": non_null_data.min().isoformat(),
                            "max": non_null_data.max().isoformat(),
                            "range_days": (non_null_data.max() - non_null_data.min()).days
                        }
                except Exception as e:
                    logger.debug(f"Could not calculate date statistics for column {column}: {e}")
            
            detailed_stats[column] = col_stats
        
        return detailed_stats
    
    @staticmethod
    def format_size(size_bytes: int) -> str:
        """Format bytes to human-readable size"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"