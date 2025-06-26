"""Core constants shared across the application.

This module contains constants that are used across multiple vertical slices
to ensure consistency and avoid duplication.
"""
from typing import Dict

# File type constants
FILE_TYPE_CSV = "csv"
FILE_TYPE_EXCEL = "xlsx"
FILE_TYPE_XLS = "xls"
FILE_TYPE_XLSM = "xlsm"
FILE_TYPE_PARQUET = "parquet"
FILE_TYPE_JSON = "json"
FILE_TYPE_XML = "xml"
FILE_TYPE_TEXT = "txt"
FILE_TYPE_BINARY = "bin"

# MIME type mappings
MIME_TYPES: Dict[str, str] = {
    FILE_TYPE_CSV: "text/csv",
    FILE_TYPE_EXCEL: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    FILE_TYPE_XLS: "application/vnd.ms-excel",
    FILE_TYPE_XLSM: "application/vnd.ms-excel.sheet.macroEnabled.12",
    FILE_TYPE_PARQUET: "application/parquet",
    FILE_TYPE_JSON: "application/json",
    FILE_TYPE_XML: "application/xml",
    FILE_TYPE_TEXT: "text/plain",
    FILE_TYPE_BINARY: "application/octet-stream",
}

# Individual MIME type constants for convenience
MIME_TYPE_CSV = MIME_TYPES[FILE_TYPE_CSV]
MIME_TYPE_EXCEL = MIME_TYPES[FILE_TYPE_EXCEL]
MIME_TYPE_PARQUET = MIME_TYPES[FILE_TYPE_PARQUET]

# Reverse mapping for convenience
FILE_EXTENSIONS: Dict[str, str] = {v: k for k, v in MIME_TYPES.items()}

# Pagination defaults
DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 1000
MAX_ROWS_PER_PAGE = 1000  # Maximum rows that can be fetched in a single request
DEFAULT_OFFSET = 0

# Domain-specific pagination overrides
DATASET_DEFAULT_PAGE_SIZE = 100  # Datasets typically need larger page sizes

# Common limits
MAX_TAG_LENGTH = 50
MAX_TAGS_PER_ENTITY = 20
MAX_NAME_LENGTH = 255
MAX_DESCRIPTION_LENGTH = 2000

# Sort defaults
DEFAULT_SORT_ORDER = "desc"
VALID_SORT_ORDERS = ["asc", "desc"]

# Compression types
COMPRESSION_NONE = None
COMPRESSION_GZIP = "gzip"
COMPRESSION_SNAPPY = "snappy"
COMPRESSION_LZ4 = "lz4"
COMPRESSION_ZSTD = "zstd"
COMPRESSION_BROTLI = "brotli"

VALID_COMPRESSION_TYPES = [
    COMPRESSION_GZIP,
    COMPRESSION_SNAPPY,
    COMPRESSION_LZ4,
    COMPRESSION_ZSTD,
    COMPRESSION_BROTLI,
]

# Status values (can be extended by specific slices)
STATUS_ACTIVE = "active"
STATUS_INACTIVE = "inactive"
STATUS_PENDING = "pending"
STATUS_PROCESSING = "processing"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_ARCHIVED = "archived"
STATUS_DELETED = "deleted"

# Time format constants
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
DATE_FORMAT = "%Y-%m-%d"

# Size formatting thresholds
KB = 1024
MB = KB * 1024
GB = MB * 1024
TB = GB * 1024

# HTTP status codes (for consistency)
HTTP_200_OK = 200
HTTP_201_CREATED = 201
HTTP_204_NO_CONTENT = 204
HTTP_400_BAD_REQUEST = 400
HTTP_401_UNAUTHORIZED = 401
HTTP_403_FORBIDDEN = 403
HTTP_404_NOT_FOUND = 404
HTTP_409_CONFLICT = 409
HTTP_422_UNPROCESSABLE_ENTITY = 422
HTTP_500_INTERNAL_SERVER_ERROR = 500