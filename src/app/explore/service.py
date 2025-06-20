import pandas as pd
import logging
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
            
        # Get file data - prefer materialized file if available
        file_id = version.materialized_file_id or version.overlay_file_id
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

    def _create_response(self, df: pd.DataFrame, request) -> Dict[str, Any]:
        """Create response with summary and optional profile"""
        # Create a simple data summary
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
