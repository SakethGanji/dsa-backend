import pandas as pd
import logging
import numpy as np
from io import BytesIO
from typing import Dict, List, Optional, Any, Tuple
from app.explore.models import ProfileFormat
from app.storage.backend import StorageBackend
from ydata_profiling import ProfileReport
import matplotlib
matplotlib.use('Agg')  # Set non-interactive backend for matplotlib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExploreService:
    def __init__(self, repository, storage_backend: StorageBackend):
        self.repository = repository
        self.storage_backend = storage_backend

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
            
            # Load file into pandas DataFrame
            df = self._load_dataframe(file_info, request.sheet)
            original_row_count = len(df)
            logger.info(f"Loaded DataFrame with {original_row_count} rows and {len(df.columns)} columns")
            
            # Apply sampling if needed
            sampled_df, sampling_info = self._apply_sampling(df, request)
            
            # Generate response
            return self._create_response(sampled_df, request, sampling_info, original_row_count)
            
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
            
        # Get file data using overlay file
        file_id = version.overlay_file_id
        file_info = await self.repository.get_file(file_id)
        if not file_info or not file_info.file_path:
            raise ValueError("File path not found")
            
        return version, file_info
    
    def _load_dataframe(self, file_info: Any, sheet_name: Optional[str] = None) -> pd.DataFrame:
        """Load file data into a pandas DataFrame"""
        # Use storage backend to read the dataset
        # We'll get dataset_id and version_id from the file_info or version info
        # For now, we'll use the file_path directly since we're transitioning
        try:
            # Since the files are stored as Parquet in the new system
            # We can read them directly using pandas
            return pd.read_parquet(file_info.file_path)
        except Exception as e:
            logger.error(f"Error loading file: {str(e)}")
            # Return an empty DataFrame with a message column
            return pd.DataFrame({"message": [f"Error loading file: {str(e)}"]})

    def _apply_sampling(self, df: pd.DataFrame, request) -> Tuple[pd.DataFrame, Optional[Dict[str, Any]]]:
        """Apply sampling to the DataFrame if needed"""
        original_rows = len(df)
        
        # Determine if sampling is needed
        should_sample = (
            request.sample_size is not None or 
            original_rows > request.auto_sample_threshold
        )
        
        if not should_sample:
            return df, None
            
        # Determine sample size
        if request.sample_size is not None:
            sample_size = min(request.sample_size, original_rows)
        else:
            # Auto-sampling: use a reasonable default based on dataset size
            sample_size = min(request.auto_sample_threshold, original_rows)
            
        logger.info(f"Sampling {sample_size} rows from {original_rows} total rows using {request.sampling_method} method")
        
        # Apply sampling method
        try:
            if request.sampling_method == "random":
                sampled_df = df.sample(n=sample_size, random_state=42)
            elif request.sampling_method == "systematic":
                # Systematic sampling: select every k-th row
                step = max(1, original_rows // sample_size)
                indices = list(range(0, original_rows, step))[:sample_size]
                sampled_df = df.iloc[indices]
            elif request.sampling_method == "stratified":
                # Simple stratified sampling based on first categorical column
                categorical_cols = df.select_dtypes(include=['object', 'category']).columns
                if len(categorical_cols) > 0:
                    strat_col = categorical_cols[0]
                    # Calculate proportional sample sizes
                    strat_counts = df[strat_col].value_counts()
                    sample_sizes = (strat_counts / original_rows * sample_size).round().astype(int)
                    
                    sampled_dfs = []
                    for value, size in sample_sizes.items():
                        if size > 0:
                            subset = df[df[strat_col] == value]
                            if len(subset) > 0:
                                sample_n = min(size, len(subset))
                                sampled_dfs.append(subset.sample(n=sample_n, random_state=42))
                    
                    if sampled_dfs:
                        sampled_df = pd.concat(sampled_dfs, ignore_index=True)
                    else:
                        # Fallback to random sampling
                        sampled_df = df.sample(n=sample_size, random_state=42)
                else:
                    # No categorical columns, fallback to random sampling
                    sampled_df = df.sample(n=sample_size, random_state=42)
            else:
                # Default to random sampling
                sampled_df = df.sample(n=sample_size, random_state=42)
                
            sampling_info = {
                "applied": True,
                "method": request.sampling_method,
                "original_rows": original_rows,
                "sampled_rows": len(sampled_df),
                "sampling_ratio": len(sampled_df) / original_rows,
                "reason": "auto_sampling" if request.sample_size is None else "user_requested"
            }
            
            return sampled_df, sampling_info
            
        except Exception as e:
            logger.warning(f"Error during sampling: {str(e)}, using full dataset")
            return df, {
                "applied": False,
                "error": str(e),
                "original_rows": original_rows
            }

    def _create_response(self, df: pd.DataFrame, request, sampling_info: Optional[Dict[str, Any]] = None, original_row_count: Optional[int] = None) -> Dict[str, Any]:
        """Create response with summary and optional profile"""
        # Create a simple data summary
        summary = {
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": list(df.columns),
            "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 * 1024),
            "sample": df.head(10).to_dict(orient="records")
        }
        
        # Add original row count if sampling was applied
        if sampling_info and sampling_info.get("applied", False):
            summary["original_rows"] = original_row_count or sampling_info.get("original_rows")

        # Only run profiling if specifically requested via a flag
        if getattr(request, 'run_profiling', False):
            # Generate a profiling report using ydata-profiling
            profile = self._generate_profile(df, request.format)

            # Format the response based on the requested format
            if request.format == ProfileFormat.HTML:
                response = {"profile": profile, "format": "html", "summary": summary}
            else:  # Default to JSON
                response = {"profile": profile, "format": "json", "summary": summary}
        else:
            # Return just the summary for faster response
            response = {"summary": summary, "format": "json", "message": "Profiling skipped. Set run_profiling=true to enable full profiling."}
        
        # Add sampling information if available
        if sampling_info:
            response["sampling_info"] = sampling_info
            
        return response
    
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
