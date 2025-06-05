import pandas as pd
import duckdb
import logging
from typing import Dict, List, Optional, Any, Tuple
from app.explore.models import ProfileFormat
from ydata_profiling import ProfileReport
import matplotlib
matplotlib.use('Agg')  # Set non-interactive backend for matplotlib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExploreService:
    def __init__(self, repository):
        self.repository = repository

    async def explore_dataset(
        self,
        dataset_id: int,
        version_id: int,
        request,
        user_id: int
    ) -> Dict[str, Any]:
        """
        Load a dataset and generate a profile report

        NOTE: This operation can be time-consuming for large datasets.
        The profiling especially can take considerable time even with minimal=True.
        """
        try:
            # Validate dataset and version
            version, file_info = await self._validate_and_get_data(dataset_id, version_id)
            
            # For large Parquet files, get metadata using DuckDB
            if file_info.file_type.lower() == "parquet":
                # Use DuckDB to get metadata efficiently
                summary = await self._get_parquet_summary_with_duckdb(file_info.file_path)
                
                # Only load data if profiling is requested
                if getattr(request, 'run_profiling', False):
                    df = self._load_dataframe(file_info, request.sheet)
                    return self._create_response(df, request, precomputed_summary=summary)
                else:
                    # Return just the summary without loading data
                    return {
                        "summary": summary,
                        "format": "json",
                        "message": "Summary generated using DuckDB. Set run_profiling=true for full profiling."
                    }
            else:
                # For non-Parquet files, use traditional approach
                df = self._load_dataframe(file_info, request.sheet)
                original_row_count = len(df)
                logger.info(f"Loaded DataFrame with {original_row_count} rows and {len(df.columns)} columns")
                
                # Generate response
                return self._create_response(df, request)
            
        except ValueError as e:
            # Specific error handling for validation errors
            logger.warning(f"Validation error in explore_dataset: {str(e)}")
            raise
        except Exception as e:
            # General error handling
            logger.error(f"Error in explore_dataset: {str(e)}", exc_info=True)
            raise
    
    async def _validate_and_get_data(self, dataset_id: int, version_id: int) -> Tuple[Any, Any]:
        """Validate dataset and version IDs and get file data"""
        # Get version info
        logger.info(f"Exploring dataset {dataset_id}, version {version_id}")
        version = await self.repository.get_dataset_version(version_id)
        if not version:
            raise ValueError(f"Dataset version with ID {version_id} not found")
            
        # Verify dataset ID matches
        if version.dataset_id != dataset_id:
            raise ValueError(f"Version {version_id} does not belong to dataset {dataset_id}")
            
        # Get file data
        file_info = await self.repository.get_file(version.file_id)
        if not file_info or not file_info.file_path:
            raise ValueError("File path not found")
        
        # Check file size to ensure we can handle it (similar to sampling)
        if hasattr(file_info, 'file_size') and file_info.file_size:
            max_file_size_gb = 50  # Maximum file size in GB
            file_size_gb = file_info.file_size / (1024 * 1024 * 1024)
            if file_size_gb > max_file_size_gb:
                raise ValueError(f"File size ({file_size_gb:.2f} GB) exceeds maximum allowed size ({max_file_size_gb} GB)")
            
        return version, file_info
    
    def _load_dataframe(self, file_info: Any, sheet_name: Optional[str] = None) -> pd.DataFrame:
        """Load file data into a pandas DataFrame using DuckDB when possible"""
        file_type = file_info.file_type.lower()
        
        try:
            # For Parquet files, use DuckDB for efficiency
            if file_type == "parquet":
                # Use DuckDB to read Parquet file efficiently
                conn = duckdb.connect(':memory:')
                try:
                    # For exploration, we limit the data to a reasonable sample
                    result = conn.execute(f"""
                        SELECT * FROM read_parquet('{file_info.file_path}')
                        LIMIT 100000
                    """).fetchdf()
                    return result
                finally:
                    conn.close()
            
            # For CSV files
            elif file_type == "csv":
                return pd.read_csv(file_info.file_path)
            
            # For Excel files
            elif file_type in ["xls", "xlsx", "xlsm"]:
                if sheet_name:
                    return pd.read_excel(file_info.file_path, sheet_name=sheet_name)
                else:
                    return pd.read_excel(file_info.file_path)
            else:
                # Default to CSV for unknown types
                return pd.read_csv(file_info.file_path)
                
        except Exception as e:
            logger.error(f"Error loading file: {str(e)}")
            # Return an empty DataFrame with a message column
            return pd.DataFrame({"message": [f"Error loading file: {str(e)}"]})

    def _create_response(self, df: pd.DataFrame, request, precomputed_summary: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create response with summary and optional profile"""
        # Use precomputed summary if available, otherwise create from DataFrame
        if precomputed_summary:
            summary = precomputed_summary
        else:
            summary = {
                "rows": len(df),
                "columns": len(df.columns),
                "column_names": list(df.columns),
                "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 * 1024),
                "sample": df.head(10).to_dict(orient="records")
            }

        # Only run profiling if specifically requested via a flag
        if getattr(request, 'run_profiling', False):
            # Generate a profiling report using ydata-profiling
            profile = self._generate_profile(df, request.format)

            # Format the response based on the requested format
            if request.format == ProfileFormat.HTML:
                return {"profile": profile, "format": "html", "summary": summary}
            else:  # Default to JSON
                return {"profile": profile, "format": "json", "summary": summary}
        else:
            # Return just the summary for faster response
            return {"summary": summary, "format": "json", "message": "Profiling skipped. Set run_profiling=true to enable full profiling."}
    
    def _generate_profile(self, df: pd.DataFrame, output_format: ProfileFormat = ProfileFormat.JSON) -> Any:
        """
        Generate a profile report for the DataFrame using ydata-profiling

        Args:
            df: The DataFrame to profile
            output_format: The desired output format (HTML or JSON)

        Returns:
            HTML string or JSON dict based on the output_format
        """
        try:
            # Create a profile report
            profile = ProfileReport(
                df,
                title="Dataset Profiling Report",
                explorative=True,
                minimal=True  # Set to True for faster but less detailed reports
            )

            # Return based on requested format
            if output_format == ProfileFormat.HTML:
                return profile.to_html()
            else:
                return profile.to_json()
        except Exception as e:
            logger.error(f"Error generating profile with ydata-profiling: {str(e)}")
            # Instead of a fallback profile, just return basic DataFrame info
            return {
                "error": f"Could not generate profile: {str(e)}",
                "rows": len(df),
                "columns": len(df.columns),
                "column_names": list(df.columns),
                "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()}
            }
    
    async def _get_parquet_summary_with_duckdb(self, file_path: str) -> Dict[str, Any]:
        """Get summary of Parquet file using DuckDB without loading all data"""
        conn = duckdb.connect(':memory:')
        try:
            # Create view from Parquet file
            conn.execute(f"CREATE VIEW dataset AS SELECT * FROM read_parquet('{file_path}')")
            
            # Get row count using COUNT(*) - DuckDB optimizes this for Parquet files
            total_rows = conn.execute("SELECT COUNT(*) FROM dataset").fetchone()[0]
            
            # Get column information
            columns_info = conn.execute("PRAGMA table_info('dataset')").fetchall()
            column_names = [col[1] for col in columns_info]
            column_types = {col[1]: col[2] for col in columns_info}
            
            # Get sample data
            sample_result = conn.execute("SELECT * FROM dataset LIMIT 10").fetchall()
            sample_data = []
            for row in sample_result:
                row_dict = {}
                for i, col_name in enumerate(column_names):
                    row_dict[col_name] = row[i]
                sample_data.append(row_dict)
            
            # Estimate memory usage
            memory_usage_mb = (total_rows * len(column_names) * 8) / (1024 * 1024)
            
            return {
                "rows": total_rows,
                "columns": len(column_names),
                "column_names": column_names,
                "column_types": column_types,
                "memory_usage_mb": round(memory_usage_mb, 2),
                "sample": sample_data
            }
        except Exception as e:
            logger.error(f"Error getting Parquet summary: {str(e)}")
            raise
        finally:
            conn.close()
