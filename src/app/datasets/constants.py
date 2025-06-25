"""Constants for the datasets slice"""

# File types
FILE_TYPE_CSV = "csv"
FILE_TYPE_EXCEL = "xlsx"
FILE_TYPE_XLS = "xls"
FILE_TYPE_XLSM = "xlsm"
FILE_TYPE_PARQUET = "parquet"

# MIME types
MIME_TYPE_CSV = "text/csv"
MIME_TYPE_EXCEL = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
MIME_TYPE_PARQUET = "application/parquet"

# Limits
# MAX_FILE_SIZE = 500 * 1024 * 1024  # 500MB - File size limit removed
MAX_ROWS_PER_PAGE = 1000
DEFAULT_PAGE_SIZE = 100
MAX_TAG_LENGTH = 50
MAX_TAGS_PER_DATASET = 20

# Sort fields
VALID_SORT_FIELDS = ["name", "created_at", "updated_at", "file_size", "current_version"]
DEFAULT_SORT_FIELD = "updated_at"
DEFAULT_SORT_ORDER = "desc"