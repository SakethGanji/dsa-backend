"""Shared type definitions for the application.

This module contains type aliases and custom types that are used
across multiple vertical slices to ensure type consistency.
"""

from typing import NewType, Union
from pathlib import Path

# Dataset-related types
DatasetId = NewType('DatasetId', str)
VersionId = NewType('VersionId', str)

# File-related types
# MUST be int to match database schema (files.id SERIAL PRIMARY KEY)
FileId = int

# Storage-related types
StoragePath = Union[str, Path]

# User-related types
UserId = NewType('UserId', str)

# Job-related types
JobId = NewType('JobId', str)
SamplingJobId = NewType('SamplingJobId', str)
AnalysisJobId = NewType('AnalysisJobId', str)