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
        # Initialize operations map
        self._operations_map = {
            "filter_rows": self._apply_filter_rows,
            "sample_rows": self._apply_sample_rows,
            "remove_columns": self._apply_remove_columns,
            "rename_columns": self._apply_rename_columns,
            "remove_nulls": self._apply_remove_nulls,
            "derive_column": self._apply_derive_column,
            "sort_rows": self._apply_sort_rows
        }
        
    async def explore_dataset(
        self,
        dataset_id: int,
        version_id: int,
        request,
        user_id: int
    ) -> Dict[str, Any]:
        """
        Apply operations to a dataset and generate a profile report
        
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
            
            # Apply operations
            df = self._apply_operations(df, request.operations)
            
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
            
    def _apply_operations(self, df: pd.DataFrame, operations: List[Dict[str, Any]]) -> pd.DataFrame:
        """Apply a series of operations to the DataFrame"""
        for op in operations:
            op_type = op.get("type")
            if op_type in self._operations_map:
                try:
                    # op is already a Dict[str, Any] as per ExploreRequest model
                    df = self._operations_map[op_type](df, op)
                except Exception as e:
                    logger.warning(f"Error applying operation {op_type}: {str(e)}")
        return df
    
    def _apply_filter_rows(self, df: pd.DataFrame, op: Dict[str, Any]) -> pd.DataFrame:
        """Apply filter_rows operation to DataFrame"""
        expression = op.get("expression", "")
        if not expression:
            return df
            
        try:
            filtered_df = df.query(expression)
            logger.info(f"Filtered rows with expression '{expression}', {len(filtered_df)} rows remaining")
            return filtered_df
        except Exception as e:
            logger.warning(f"Error applying filter: {str(e)}")
            return df
    
    def _apply_sample_rows(self, df: pd.DataFrame, op: Dict[str, Any]) -> pd.DataFrame:
        """Apply sample_rows operation to DataFrame"""
        # Get sample fraction (support both "fraction" and "frac" keys for backward compatibility)
        if "fraction" in op:
            fraction = float(op.get("fraction", 0.1))
        else:
            fraction = float(op.get("frac", 0.1))
            
        method = op.get("method", "random")
        
        # Make sure fraction is between 0.01 and 1.0
        fraction = max(0.01, min(1.0, fraction))
        
        # If DataFrame is very small, just return all rows
        if len(df) <= 100:
            return df
            
        try:
            if method == "random":
                sampled_df = df.sample(frac=fraction)
            elif method == "head":
                sampled_df = df.head(int(len(df) * fraction))
            elif method == "tail":
                sampled_df = df.tail(int(len(df) * fraction))
            else:
                # Default to random sampling
                sampled_df = df.sample(frac=fraction)
                
            logger.info(f"Sampled rows with {method} method, fraction {fraction}, {len(sampled_df)} rows selected")
            return sampled_df
        except Exception as e:
            logger.warning(f"Error sampling rows: {str(e)}")
            return df
    
    def _apply_remove_columns(self, df: pd.DataFrame, op: Dict[str, Any]) -> pd.DataFrame:
        """Apply remove_columns operation to DataFrame"""
        columns = op.get("columns", [])
        if not columns:
            return df
            
        try:
            # Only drop columns that exist in the DataFrame
            columns_to_drop = [col for col in columns if col in df.columns]
            if columns_to_drop:
                result_df = df.drop(columns=columns_to_drop)
                logger.info(f"Removed columns: {columns_to_drop}, {len(result_df.columns)} columns remaining")
                return result_df
            return df
        except Exception as e:
            logger.warning(f"Error removing columns: {str(e)}")
            return df
    
    def _apply_rename_columns(self, df: pd.DataFrame, op: Dict[str, Any]) -> pd.DataFrame:
        """Apply rename_columns operation to DataFrame"""
        mappings = op.get("mappings", {})
        if not mappings:
            return df
            
        try:
            # Only rename columns that exist in the DataFrame
            valid_mappings = {k: v for k, v in mappings.items() if k in df.columns}
            if valid_mappings:
                renamed_df = df.rename(columns=valid_mappings)
                logger.info(f"Renamed columns: {valid_mappings}")
                return renamed_df
            return df
        except Exception as e:
            logger.warning(f"Error renaming columns: {str(e)}")
            return df
    
    def _apply_remove_nulls(self, df: pd.DataFrame, op: Dict[str, Any]) -> pd.DataFrame:
        """Apply remove_nulls operation to DataFrame"""
        columns = op.get("columns", [])
        
        try:
            if columns:
                # Only consider columns that exist in the DataFrame
                valid_columns = [col for col in columns if col in df.columns]
                if valid_columns:
                    result_df = df.dropna(subset=valid_columns)
                    logger.info(f"Removed null values in columns {valid_columns}, {len(result_df)} rows remaining")
                    return result_df
                return df
            else:
                # If no columns specified, remove rows with any nulls
                result_df = df.dropna()
                logger.info(f"Removed rows with any null values, {len(result_df)} rows remaining")
                return result_df
        except Exception as e:
            logger.warning(f"Error removing nulls: {str(e)}")
            return df
    
    def _apply_derive_column(self, df: pd.DataFrame, op: Dict[str, Any]) -> pd.DataFrame:
        """Apply derive_column operation to DataFrame"""
        column_name = op.get("column", "")
        expression = op.get("expression", "")
        
        if not column_name or not expression:
            return df
            
        try:
            # Use safer evaluation for derived columns
            # Limit the scope of what can be executed
            local_vars = {"df": df}
            result_df = df.copy()
            result_df[column_name] = eval(expression, {"__builtins__": {}}, local_vars)
            logger.info(f"Created derived column '{column_name}'")
            return result_df
        except Exception as e:
            logger.warning(f"Error creating derived column: {str(e)}")
            return df
    
    def _apply_sort_rows(self, df: pd.DataFrame, op: Dict[str, Any]) -> pd.DataFrame:
        """Apply sort_rows operation to DataFrame"""
        columns = op.get("columns", [])
        order = op.get("order", [])
        
        if not columns:
            return df
            
        try:
            # Validate columns exist
            valid_columns = [col for col in columns if col in df.columns]
            if not valid_columns:
                return df
                
            # Convert order strings to boolean (True for ascending)
            ascending = []
            for i, col in enumerate(valid_columns):
                if i < len(order):
                    ascending.append(order[i].lower() == "asc")
                else:
                    ascending.append(True)  # Default to ascending
            
            sorted_df = df.sort_values(by=valid_columns, ascending=ascending)
            logger.info(f"Sorted rows by columns: {valid_columns} with order {ascending}")
            return sorted_df
        except Exception as e:
            logger.warning(f"Error sorting rows: {str(e)}")
            return df
    
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
