import pandas as pd
import numpy as np
import logging
import json
from io import BytesIO, StringIO
from typing import Dict, List, Optional, Any, Union
from app.explore.models import ProfileFormat
import ydata_profiling
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
        Apply operations to a dataset and generate a profile report
        
        NOTE: This operation can be time-consuming for large datasets.
        The profiling especially can take considerable time even with minimal=True.
        """
        try:
            # Get version info
            logger.info(f"Exploring dataset {dataset_id}, version {version_id}")
            version = await self.repository.get_dataset_version(version_id)
            if not version:
                raise ValueError(f"Dataset version with ID {version_id} not found")
                
            # Verify dataset ID matches
            if version["dataset_id"] != dataset_id:
                raise ValueError(f"Version {version_id} does not belong to dataset {dataset_id}")
                
            # Get file data
            file_info = await self.repository.get_file(version["file_id"])
            if not file_info or not file_info.get("file_data"):
                raise ValueError("File data not found")
            
            # Load file into pandas DataFrame
            df = self._load_dataframe(file_info, request.sheet)
            original_row_count = len(df)
            logger.info(f"Loaded DataFrame with {original_row_count} rows and {len(df.columns)} columns")
            
            # Apply operations
            for op in request.operations:
                op_type = op.get("type")
                
                if op_type == "filter_rows":
                    expression = op.get("expression", "")
                    if expression:
                        try:
                            df = df.query(expression)
                            logger.info(f"Filtered rows with expression '{expression}', {len(df)} rows remaining")
                        except Exception as e:
                            logger.warning(f"Error applying filter: {str(e)}")
                
                elif op_type == "sample_rows":
                    if "fraction" in op:
                        fraction = float(op.get("fraction", 0.1))
                    else:
                        fraction = float(op.get("frac", 0.1))
                        
                    method = op.get("method", "random")
                    try:
                        # Make sure fraction is between 0 and 1
                        fraction = max(0.01, min(1.0, fraction))
                        # If DataFrame is very small, just return all rows
                        if len(df) <= 100:
                            pass
                        else:
                            if method == "random":
                                df = df.sample(frac=fraction)
                            elif method == "head":
                                df = df.head(int(len(df) * fraction))
                            elif method == "tail":
                                df = df.tail(int(len(df) * fraction))
                                
                        logger.info(f"Sampled rows with {method} method, fraction {fraction}, {len(df)} rows selected")
                    except Exception as e:
                        logger.warning(f"Error sampling rows: {str(e)}")
                
                elif op_type == "remove_columns":
                    columns = op.get("columns", [])
                    if columns:
                        try:
                            # Only keep columns that exist
                            columns_to_drop = [col for col in columns if col in df.columns]
                            if columns_to_drop:
                                df = df.drop(columns=columns_to_drop)
                                logger.info(f"Removed columns: {columns_to_drop}, {len(df.columns)} columns remaining")
                        except Exception as e:
                            logger.warning(f"Error removing columns: {str(e)}")
                
                elif op_type == "rename_columns":
                    mappings = op.get("mappings", {})
                    if mappings:
                        try:
                            # Only rename columns that exist
                            valid_mappings = {k: v for k, v in mappings.items() if k in df.columns}
                            if valid_mappings:
                                df = df.rename(columns=valid_mappings)
                                logger.info(f"Renamed columns: {valid_mappings}")
                        except Exception as e:
                            logger.warning(f"Error renaming columns: {str(e)}")
                
                elif op_type == "remove_nulls":
                    columns = op.get("columns", [])
                    try:
                        if columns:
                            # Only consider columns that exist in the DataFrame
                            valid_columns = [col for col in columns if col in df.columns]
                            if valid_columns:
                                df = df.dropna(subset=valid_columns)
                        else:
                            # If no columns specified, remove rows with any nulls
                            df = df.dropna()
                        
                        logger.info(f"Removed null values, {len(df)} rows remaining")
                    except Exception as e:
                        logger.warning(f"Error removing nulls: {str(e)}")
                
                elif op_type == "derive_column":
                    column_name = op.get("column", "")
                    expression = op.get("expression", "")
                    if column_name and expression:
                        try:
                            # Use safer evaluation for derived columns
                            # Limit the scope of what can be executed
                            local_vars = {"df": df}
                            df[column_name] = eval(expression, {"__builtins__": {}}, local_vars)
                            logger.info(f"Created derived column '{column_name}'")
                        except Exception as e:
                            logger.warning(f"Error creating derived column: {str(e)}")
                
                elif op_type == "sort_rows":
                    columns = op.get("columns", [])
                    order = op.get("order", [])
                    if columns:
                        try:
                            # Validate columns exist
                            valid_columns = [col for col in columns if col in df.columns]
                            if valid_columns:
                                # Convert order strings to boolean (True for ascending)
                                ascending = []
                                for i, col in enumerate(valid_columns):
                                    if i < len(order):
                                        ascending.append(order[i].lower() == "asc")
                                    else:
                                        ascending.append(True)  # Default to ascending
                                
                                df = df.sort_values(by=valid_columns, ascending=ascending)
                                logger.info(f"Sorted rows by columns: {valid_columns}")
                        except Exception as e:
                            logger.warning(f"Error sorting rows: {str(e)}")
            
            # Create a simple data summary before the expensive profiling
            summary = {
                "rows": len(df),
                "columns": len(df.columns),
                "column_names": list(df.columns),
                "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 * 1024),
                "sample": df.head(10).to_dict(orient="records")
            }
            
            # Only run profiling if specifically requested via a flag
            if getattr(request, 'run_profiling', False):  # Default to False for performance
                # Generate a profiling report using ydata-profiling (can be slow)
                profile = self._generate_profile(df, request.format)
                
                # Format the response based on the requested format
                if request.format == ProfileFormat.HTML:
                    return {"profile": profile, "format": "html", "summary": summary}
                else:  # Default to JSON
                    return {"profile": profile, "format": "json", "summary": summary}
            else:
                # Return just the summary for faster response
                return {"summary": summary, "format": "json", "message": "Profiling skipped for performance reasons"}
            
        except Exception as e:
            logger.error(f"Error in explore_dataset: {str(e)}", exc_info=True)
            raise
            
    def _load_dataframe(self, file_info: Dict[str, Any], sheet_name: Optional[str] = None) -> pd.DataFrame:
        """Load file data into a pandas DataFrame"""
        file_data = file_info["file_data"]
        file_type = file_info["file_type"].lower()
        
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
            # Create a profile report with minimal configuration for speed
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
            # Fallback to simple profile if ydata-profiling fails
            return self._generate_simple_profile(df)
        
    def _calculate_histogram(self, series: pd.Series) -> Dict[str, Any]:
        """Calculate histogram data for a numeric series"""
        try:
            # Drop nulls and convert to numpy array
            data = series.dropna().to_numpy()
            if len(data) == 0:
                return {"counts": [], "bin_edges": []}
            
            # Calculate histogram
            counts, bin_edges = np.histogram(data, bins=10)
            
            # Convert to Python types for JSON serialization
            return {
                "counts": counts.tolist(),
                "bin_edges": bin_edges.tolist()
            }
        except:
            return {"counts": [], "bin_edges": []}
            
    def _generate_simple_profile(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate a simple profile report for the DataFrame (fallback method)"""
        # Limit sample size for profiling
        df_sample = df.sample(min(1000, len(df))) if len(df) > 1000 else df
        
        # Basic statistics
        profile = {
            "table": {
                "n": len(df),
                "n_var": len(df.columns),
                "n_cells_missing": int(df.isna().sum().sum()),
                "memory_size": int(df.memory_usage(deep=True).sum()),
                "record_size": int(df.memory_usage(deep=True).sum() / len(df)) if len(df) > 0 else 0,
                "columns": list(df.columns)
            },
            "variables": {},
            "correlations": {},
            "sample_data": []
        }
        
        # Generate variable statistics
        for col in df.columns:
            col_stats = {
                "count": len(df),
                "n_missing": int(df[col].isna().sum()),
                "p_missing": float(df[col].isna().mean()),
                "type": str(df[col].dtype),
                "unique": int(df[col].nunique())
            }
            
            # Add type-specific statistics
            if pd.api.types.is_numeric_dtype(df[col]):
                # For numeric columns
                if not df[col].isna().all():
                    col_stats.update({
                        "min": float(df[col].min()),
                        "max": float(df[col].max()),
                        "mean": float(df[col].mean()),
                        "std": float(df[col].std()),
                        "median": float(df[col].median()),
                        "range": float(df[col].max() - df[col].min()),
                        "histogram": self._calculate_histogram(df[col])
                    })
            
            elif pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]):
                # For string/object columns
                value_counts = df_sample[col].value_counts(dropna=False).head(10)
                value_counts_dict = {}
                for k, v in value_counts.items():
                    key = str(k) if k is not None else "null"
                    value_counts_dict[key] = int(v)
                
                col_stats.update({
                    "top": str(value_counts.index[0]) if len(value_counts) > 0 else None,
                    "freq": int(value_counts.iloc[0]) if len(value_counts) > 0 else 0,
                    "value_counts": value_counts_dict
                })
            
            elif pd.api.types.is_datetime64_dtype(df[col]):
                # For datetime columns
                if not df[col].isna().all():
                    col_stats.update({
                        "min": str(df[col].min()),
                        "max": str(df[col].max())
                    })
            
            profile["variables"][col] = col_stats
        
        # Calculate correlations between numeric columns
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 1:
            try:
                corr_matrix = df[numeric_cols].corr(method='pearson')
                profile["correlations"]["pearson"] = corr_matrix.to_dict()
            except:
                # If correlation calculation fails, provide empty dict
                profile["correlations"]["pearson"] = {}
        
        # Add sample data (first 10 rows)
        sample_rows = min(10, len(df))
        try:
            profile["sample_data"] = df.head(sample_rows).to_dict(orient='records')
        except:
            # Handle any serialization issues
            sample_data = []
            for i in range(sample_rows):
                row_dict = {}
                for col in df.columns:
                    try:
                        value = df.iloc[i][col]
                        row_dict[col] = str(value) if value is not None else None
                    except:
                        row_dict[col] = None
                sample_data.append(row_dict)
            profile["sample_data"] = sample_data
        
        return profile
