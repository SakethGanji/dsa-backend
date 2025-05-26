import pandas as pd
import logging
from io import BytesIO
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
            
        # Get file data
        file_info = await self.repository.get_file(version.file_id)
        if not file_info or not file_info.file_data:
            raise ValueError("File data not found")
            
        return version, file_info
    
    def _load_dataframe(self, file_info: Any, sheet_name: Optional[str] = None) -> pd.DataFrame:
        """Load file data into a pandas DataFrame"""
        file_data = file_info.file_data
        file_type = file_info.file_type.lower()

        # Create BytesIO object from file data
        buffer = BytesIO(file_data)
        
        try:
            if file_type == "csv":
                return pd.read_csv(buffer)
            elif file_type in ["xls", "xlsx", "xlsm"]:
                if sheet_name:
                    return pd.read_excel(buffer, sheet_name=sheet_name)
                else:
                    # If no sheet name provided, use the first sheet
                    return pd.read_excel(buffer)
            else:
                # Just try csv as a fallback
                return pd.read_csv(buffer)
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
