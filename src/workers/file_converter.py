"""File conversion module for converting CSV/Excel to Parquet format."""

import os
import json
import time
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any
import tempfile
import asyncio
from datetime import datetime

import polars as pl
import aiofiles
import aiofiles.os


class FileConverter:
    """Converts various file formats to standardized Parquet format."""
    
    def __init__(self, compression: str = 'zstd', batch_size: int = 100_000):
        self.compression = compression
        self.batch_size = batch_size
        self.large_file_threshold = 1_000_000_000  # 1GB
    
    async def convert_to_parquet(
        self,
        source_path: str,
        output_dir: str,
        original_filename: str
    ) -> Tuple[List[Tuple[str, str]], Dict[str, Any]]:
        """
        Convert a source file to one or more Parquet files.
        
        Returns:
            - List of (table_key, parquet_path) tuples
            - Conversion metadata dictionary
        """
        start_time = time.time()
        source_path = Path(source_path)
        output_dir = Path(output_dir)
        file_ext = source_path.suffix.lower()
        
        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Track original file size
        original_size = source_path.stat().st_size
        
        # Initialize progress tracking
        progress_file = output_dir / ".conversion_progress.json"
        completed_tables: List[str] = []
        
        # Load existing progress if resuming
        if progress_file.exists():
            with open(progress_file, 'r') as f:
                progress_data = json.load(f)
                completed_tables = progress_data.get('completed', [])
        
        converted_files: List[Tuple[str, str]] = []
        conversion_errors: List[Dict[str, str]] = []
        
        try:
            if file_ext == '.csv':
                result = await self._convert_csv(
                    source_path, output_dir, original_size, completed_tables
                )
                converted_files.extend(result)
                
            elif file_ext == '.xlsx':
                result = await self._convert_excel(
                    source_path, output_dir, original_filename, completed_tables, progress_file
                )
                converted_files.extend(result['files'])
                conversion_errors.extend(result.get('errors', []))
                
            elif file_ext == '.parquet':
                # Already in target format
                converted_files.append(('primary', str(source_path)))
                
            else:
                raise ValueError(f"Unsupported file format: {file_ext}")
            
        finally:
            # Clean up progress file on success
            if progress_file.exists() and not conversion_errors:
                progress_file.unlink()
        
        # Calculate conversion metadata
        conversion_time = time.time() - start_time
        metadata = self._create_conversion_metadata(
            original_filename, file_ext, original_size, 
            converted_files, conversion_time, conversion_errors
        )
        
        return converted_files, metadata
    
    async def _convert_csv(
        self,
        source_path: Path,
        output_dir: Path,
        original_size: int,
        completed_tables: List[str]
    ) -> List[Tuple[str, str]]:
        """Convert CSV file to Parquet using Polars' optimized reading."""
        table_key = 'primary'
        
        # Skip if already converted
        if table_key in completed_tables:
            output_path = output_dir / f"{source_path.stem}.parquet"
            if output_path.exists():
                return [(table_key, str(output_path))]
        
        output_path = output_dir / f"{source_path.stem}.parquet"
        
        # Run conversion in thread pool to avoid blocking
        loop = asyncio.get_running_loop()
        
        # Polars handles large files efficiently, so we can use the same method
        await loop.run_in_executor(
            None, self._convert_csv_optimized, source_path, output_path
        )
        
        return [(table_key, str(output_path))]
    
    def _convert_csv_optimized(self, source_path: Path, output_path: Path):
        """Optimized CSV conversion using Polars' native capabilities."""
        # For very large files, use lazy evaluation with sink_parquet
        file_size = source_path.stat().st_size
        
        if file_size > self.large_file_threshold:
            # Use lazy evaluation for huge files
            lazy_df = pl.scan_csv(source_path, infer_schema_length=10000)
            
            # Apply transformations lazily
            # Note: Schema normalization needs to be done differently with lazy frames
            lazy_df.sink_parquet(output_path, compression=self.compression)
            
            # Post-process to normalize schema if needed
            # This is a trade-off: we get streaming but lose some normalization
            # For most use cases, this is acceptable
        else:
            # For smaller files, use eager evaluation with full normalization
            df = pl.read_csv(source_path, infer_schema_length=10000)
            
            # Validate and normalize schema
            df = self._validate_and_normalize_schema(df, 'primary')
            
            # Write to Parquet
            df.write_parquet(output_path, compression=self.compression)
    
    async def _convert_excel(
        self,
        source_path: Path,
        output_dir: Path,
        original_filename: str,
        completed_tables: List[str],
        progress_file: Path
    ) -> Dict[str, Any]:
        """Convert Excel file to multiple Parquet files (one per sheet)."""
        converted_files = []
        conversion_errors = []
        
        # Run in thread pool
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None, self._convert_excel_sync, 
            source_path, output_dir, original_filename, 
            completed_tables, progress_file
        )
        
        return result
    
    def _convert_excel_sync(
        self,
        source_path: Path,
        output_dir: Path,
        original_filename: str,
        completed_tables: List[str],
        progress_file: Path
    ) -> Dict[str, Any]:
        """Synchronous Excel conversion using Polars direct reading."""
        converted_files = []
        conversion_errors = []
        
        try:
            # Read all sheets at once using Polars
            # This is more efficient than opening/closing the file multiple times
            all_sheets = pl.read_excel(
                source_path, 
                sheet_id=None,  # None means read all sheets
                read_csv_options={'infer_schema_length': 10000}
            )
            
            # Process each sheet
            for sheet_name, df in all_sheets.items():
                if sheet_name in completed_tables:
                    # Check if file exists from previous run
                    safe_name = self._sanitize_filename(sheet_name)
                    output_path = output_dir / f"{source_path.stem}-{safe_name}.parquet"
                    if output_path.exists():
                        converted_files.append((sheet_name, str(output_path)))
                        continue
                
                try:
                    if df.is_empty():
                        continue
                    
                    # Validate and normalize schema
                    df = self._validate_and_normalize_schema(df, sheet_name)
                    
                    # Create output path
                    safe_name = self._sanitize_filename(sheet_name)
                    output_path = output_dir / f"{source_path.stem}-{safe_name}.parquet"
                    
                    # Write to Parquet
                    df.write_parquet(output_path, compression=self.compression)
                    
                    converted_files.append((sheet_name, str(output_path)))
                    completed_tables.append(sheet_name)
                    
                    # Update progress
                    with open(progress_file, 'w') as f:
                        json.dump({'completed': completed_tables}, f)
                    
                except Exception as e:
                    conversion_errors.append({
                        'sheet_name': sheet_name,
                        'error': str(e)
                    })
            
        except (pl.exceptions.InvalidOperationError, zipfile.BadZipFile) as e:
            # Polars raises different exceptions than openpyxl
            raise ValueError(
                f"Failed to open Excel file '{original_filename}'. "
                f"It may be corrupt or an unsupported format. Error: {e}"
            )
        
        return {
            'files': converted_files,
            'errors': conversion_errors
        }
    
    def _validate_and_normalize_schema(self, df: pl.DataFrame, table_key: str) -> pl.DataFrame:
        """Validate and normalize DataFrame schema."""
        # Remove completely empty columns
        df = df.select([col for col in df.columns if not df[col].is_null().all()])
        
        # Normalize column names (remove special characters, lowercase)
        df = df.rename(lambda col: self._normalize_column_name(col))
        
        # Infer better types for string columns that might be numeric
        for col in df.columns:
            if df[col].dtype == pl.Utf8:
                # Try to cast to numeric types
                try:
                    # Check if it's integer-like
                    if df[col].drop_nulls().str.match(r'^-?\d+$').all():
                        df = df.with_columns(pl.col(col).cast(pl.Int64))
                    # Check if it's float-like
                    elif df[col].drop_nulls().str.match(r'^-?\d*\.?\d+$').all():
                        df = df.with_columns(pl.col(col).cast(pl.Float64))
                except:
                    # Keep as string if casting fails
                    pass
        
        return df
    
    def _normalize_column_name(self, name: str) -> str:
        """Normalize column name to be SQL-friendly."""
        # Replace spaces and special characters with underscores
        import re
        normalized = re.sub(r'[^\w\s]', '', name)
        normalized = re.sub(r'\s+', '_', normalized)
        return normalized.lower()
    
    def _sanitize_filename(self, name: str) -> str:
        """Sanitize string to be a valid filename."""
        return "".join(c for c in name if c.isalnum() or c in (' ', '_', '-')).rstrip()
    
    def _create_conversion_metadata(
        self,
        original_filename: str,
        original_format: str,
        original_size: int,
        converted_files: List[Tuple[str, str]],
        conversion_time: float,
        errors: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        """Create detailed conversion metadata."""
        metadata = {
            'original_filename': original_filename,
            'original_format': original_format,
            'original_size_bytes': original_size,
            'conversion_time_seconds': round(conversion_time, 2),
            'conversion_timestamp': datetime.utcnow().isoformat(),
            'tables_converted': len(converted_files),
            'conversion_errors': errors,
            'parquet_files': []
        }
        
        # Add details for each converted file
        for table_key, parquet_path in converted_files:
            if os.path.exists(parquet_path):
                parquet_size = os.path.getsize(parquet_path)
                
                # Get row count and schema info
                try:
                    df_info = pl.scan_parquet(parquet_path).select([
                        pl.count().alias('row_count')
                    ]).collect()
                    row_count = df_info['row_count'][0]
                    
                    # Get schema
                    schema = pl.scan_parquet(parquet_path).schema
                    
                    metadata['parquet_files'].append({
                        'table_key': table_key,
                        'path': parquet_path,
                        'size_bytes': parquet_size,
                        'compression_ratio': round(original_size / parquet_size, 2) if parquet_size > 0 else 0,
                        'row_count': row_count,
                        'column_count': len(schema),
                        'columns': [{'name': name, 'type': str(dtype)} for name, dtype in schema.items()]
                    })
                except Exception as e:
                    # If we can't read the file, just record basic info
                    metadata['parquet_files'].append({
                        'table_key': table_key,
                        'path': parquet_path,
                        'size_bytes': parquet_size,
                        'error': f"Could not read file info: {str(e)}"
                    })
        
        return metadata