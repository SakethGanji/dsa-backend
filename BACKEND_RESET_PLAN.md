# Backend Reset Plan for Git-like Dataset Versioning System

## Overview
This plan outlines the strategy for resetting the backend implementation while preserving all API contracts. The new implementation will use a Git-like content-addressable storage system for dataset versioning.

## Key Architectural Changes

### Old Architecture → New Architecture Mapping
| Old System | New System | Description |
|------------|------------|-------------|
| `files` table | `rows` table | Content-addressable JSONB row storage |
| `dataset_versions` table | `commits` table | Version graph with parent pointers |
| `dataset_version_files` table | `commit_rows` table | Manifest of rows per commit |
| `version_tags` table | `refs` table | Named pointers to commits |
| File storage (Parquet) | JSONB in `rows` table | Data stored directly in DB |
| Sequential versioning | Git-like DAG | Branching/merging support |
| DuckDB queries | PostgreSQL JSONB queries | Native JSON operations |

## Implementation Reset Strategy

### 1. Core Infrastructure (`app/core/`)

#### Files to Keep As-Is:
- `config.py` - Configuration structure remains valid
- `logging_config.py` - No changes needed

#### Files to Hollow Out:
None - core infrastructure is implementation-agnostic

#### Notes:
- `events/` directory is empty and can be removed

### 2. Database Layer (`app/db/`)

#### Files to Modify:
- **`connection.py`** - Keep structure, same async SQLAlchemy setup
- **`bootstrap.py`** - Update to use new schema, keep parsing logic

### 3. Datasets Vertical (`app/datasets/`)

#### Files to Keep (Routes Only):
- **`routes.py`** - All endpoints remain with same signatures
- **`models.py`** - Keep all Pydantic models for API contracts
- **`validators.py`** - Keep validation logic
- **`exceptions.py`** - Keep exception definitions
- **`constants.py`** - Keep constants

#### Files to Remove:
- **`duckdb_service.py`** - No longer needed, using PostgreSQL JSONB queries

#### Files to Hollow Out:

##### `service.py` - Dataset Service Functions
```python
class DatasetService:
    async def create_dataset(self, name: str, description: str, created_by: int) -> Dataset:
        """
        Creates a new dataset entity with initial commit.
        
        Implementation Notes:
        1. Create dataset record in `datasets` table
        2. Create initial empty commit with message "Initial commit"
        3. Create 'main' ref pointing to initial commit
        4. Grant admin permission to creator
        
        Returns: Dataset object with ID and metadata
        """
        raise NotImplementedError("Implement using new schema")

    async def upload_dataset_version(
        self, dataset_id: int, file: UploadFile, user_id: int, tags: List[str]
    ) -> DatasetVersion:
        """
        Creates new commit with uploaded data.
        
        Implementation Notes:
        1. Parse uploaded file (CSV/Excel/Parquet)
        2. Convert each row to JSONB, calculate SHA256 hash
        3. Store unique rows in `rows` table
        4. Create new commit with parent = current 'main' ref
        5. Create entries in `commit_rows` for all rows
        6. Update 'main' ref to new commit
        7. Extract and store schema in `commit_schemas`
        
        Returns: Version info mapped from commit
        """
        raise NotImplementedError("Implement using new schema")

    async def get_dataset_data(
        self, dataset_id: int, version_id: int, sheet_name: str, limit: int, offset: int
    ) -> Dict[str, Any]:
        """
        Retrieves paginated data from a specific commit.
        
        Implementation Notes:
        1. Find commit by mapping version_id
        2. Join `commit_rows` with `rows` to get data
        3. Order by logical_row_id for consistent pagination
        4. Return data as list of dicts from JSONB
        5. Note: sheet_name is legacy - all data is now in one logical table
        
        Returns: {"data": [...], "total": count, "sheet": name}
        """
        raise NotImplementedError("Implement using new schema")

    async def create_version_from_changes(
        self, dataset_id: int, base_version_id: int, changes: Dict
    ) -> DatasetVersion:
        """
        Apply overlay changes to create new version.
        
        Implementation Notes:
        1. Find base commit from version_id
        2. Get all rows from base commit
        3. Apply changes:
           - add: Insert new rows (hash, store in `rows`)
           - remove: Exclude rows by logical_row_id
           - update: Replace rows with new hashed versions
        4. Create new commit with updated manifest
        5. Update 'main' ref
        
        Returns: New version info
        """
        raise NotImplementedError("Implement using new schema")

    async def tag_version(self, dataset_id: int, version_id: int, tag_name: str):
        """
        Creates a named ref pointing to a commit.
        
        Implementation Notes:
        1. Find commit from version_id
        2. Create/update entry in `refs` table
        3. Ensure uniqueness per dataset
        
        Note: Tags like 'production', 'v1.0' become refs
        """
        raise NotImplementedError("Implement using new schema")
```

##### `repository.py` - Dataset Repository Functions
```python
class DatasetRepository:
    # Core dataset operations
    async def create_dataset(self, conn, name: str, description: str, created_by: int) -> int:
        """
        SQL: INSERT INTO datasets, return ID
        """
        raise NotImplementedError()

    async def create_commit(
        self, conn, dataset_id: int, parent_id: str, message: str, author_id: int
    ) -> str:
        """
        SQL: INSERT INTO commits with SHA256 commit_id
        Commit ID = hash of (dataset_id + parent_id + message + timestamp)
        """
        raise NotImplementedError()

    async def store_row(self, conn, data: Dict) -> str:
        """
        SQL: INSERT INTO rows ON CONFLICT DO NOTHING
        Returns: SHA256 hash of JSON-serialized data
        """
        raise NotImplementedError()

    async def link_commit_rows(
        self, conn, commit_id: str, rows: List[Tuple[str, str]]
    ):
        """
        SQL: Bulk INSERT INTO commit_rows (commit_id, logical_row_id, row_hash)
        """
        raise NotImplementedError()

    async def update_ref(self, conn, dataset_id: int, ref_name: str, commit_id: str):
        """
        SQL: INSERT INTO refs ... ON CONFLICT UPDATE
        """
        raise NotImplementedError()

    async def get_commit_data(
        self, conn, commit_id: str, limit: int, offset: int
    ) -> List[Dict]:
        """
        SQL: SELECT r.data FROM commit_rows cr 
             JOIN rows r ON cr.row_hash = r.row_hash
             WHERE cr.commit_id = ? 
             ORDER BY cr.logical_row_id
             LIMIT ? OFFSET ?
        """
        raise NotImplementedError()

    # Version mapping helpers (temporary during migration)
    async def get_version_number_for_commit(self, conn, commit_id: str) -> int:
        """
        Maps commit to version number by counting ancestors
        """
        raise NotImplementedError()

    async def get_commit_for_version_number(
        self, conn, dataset_id: int, version_num: int
    ) -> str:
        """
        Maps version number to commit by walking history
        """
        raise NotImplementedError()
```


##### `statistics_service.py` - Statistics Calculation
```python
class StatisticsService:
    async def calculate_statistics(self, commit_id: str) -> Dict[str, Any]:
        """
        Calculate statistics for a commit.
        
        Implementation Notes:
        1. Count rows in commit_rows
        2. Calculate size from rows content
        3. Use PostgreSQL JSONB functions for column statistics:
           - Extract columns using jsonb_object_keys
           - Numeric: Calculate min, max, avg using JSONB operators
           - Categorical: COUNT DISTINCT on JSONB fields
           - Use jsonb_typeof to detect data types
           - Null counts using JSONB IS NULL checks
        4. Store in commit_statistics table
        
        Returns: Complete statistics dictionary
        """
        raise NotImplementedError()
```

##### `controller.py` - Dataset Controller
```python
class DatasetsController:
    """Controller layer handling HTTP requests/responses for datasets"""
    
    def __init__(self, service: DatasetsService):
        self.service = service

    def _parse_tags(self, tags: Optional[str]) -> Optional[List[str]]:
        """
        Parse tags from JSON or comma-separated string.
        
        Implementation Notes:
        1. Try JSON.parse first
        2. Fallback to comma-split
        3. Raise HTTPException on invalid format
        
        Keep exact same parsing logic for UI compatibility
        """
        raise NotImplementedError()

    async def upload_dataset(
        self,
        file: UploadFile,
        current_user: Any,
        dataset_id: Optional[int],
        name: str,
        description: Optional[str],
        tags: Optional[str],
    ) -> DatasetUploadResponse:
        """
        Handle dataset upload request.
        
        Implementation Notes:
        1. Validate file type/size
        2. Parse tags using _parse_tags
        3. Call service.upload_dataset_version
        4. Transform service response to HTTP response
        5. Handle service exceptions → HTTP errors
        
        Error Mapping:
        - FileProcessingError → 400
        - DatasetNotFound → 404
        - StorageError → 500
        """
        raise NotImplementedError()

    async def create_version_from_changes(
        self,
        dataset_id: int,
        request: VersionCreateRequest,
        current_user: Any
    ) -> VersionCreateResponse:
        """
        Create new version from overlay changes.
        
        Implementation Notes:
        1. Validate request.changes format
        2. Call service.create_version_from_changes
        3. Transform response maintaining same structure
        4. Include backwards-compatible version_number
        """
        raise NotImplementedError()
```

### 4. Dataset Search Vertical (`app/datasets/search/`)

#### Files to Keep:
- **`routes.py`** - Search endpoints remain
- **`models.py`** - Search request/response models

#### Files to Hollow Out:

##### `service.py` - Search Service
```python
class DatasetSearchService:
    async def search_datasets(self, params: SearchRequest) -> SearchResponse:
        """
        Full-text search across datasets with faceting.
        
        Implementation Notes:
        1. Build query with FTS on name/description
        2. Apply filters (tags, creators, dates)
        3. Check permissions for user
        4. Calculate facets from results
        5. Return paginated results
        
        Note: Search works on dataset level, not commit level
        """
        raise NotImplementedError()

    async def get_suggestions(self, query: str, limit: int) -> List[str]:
        """
        Autocomplete suggestions.
        
        Implementation Notes:
        1. Use pg_trgm for similarity matching
        2. Search dataset names and tags
        3. Return ranked suggestions
        """
        raise NotImplementedError()
```

##### `repository.py` - Search Repository
```python
class DatasetSearchRepository:
    async def search_datasets_with_facets(
        self, conn, query: str, filters: Dict, limit: int, offset: int
    ) -> Tuple[List[Dict], Dict[str, List]]:
        """
        Full-text search with faceted results.
        
        SQL Strategy:
        1. Use PostgreSQL FTS with to_tsquery
        2. JOIN with tags, users for faceting
        3. Apply permission filters
        4. Calculate facet counts in single query using CTEs
        
        Returns: (results, facets)
        """
        raise NotImplementedError()
```

### 5. Explore Vertical (`app/explore/`)

#### Files to Keep:
- **`routes.py`** - Exploration endpoints
- **`models.py`** - Request/response models

#### Files to Hollow Out:

##### `service.py` - Explore Service
```python
class ExploreService:
    async def explore_dataset(
        self, dataset_id: int, version_id: int, params: ExploreRequest
    ) -> Dict[str, Any]:
        """
        Generate profile report for dataset commit.
        
        Implementation Notes:
        1. Map version_id to commit_id
        2. Load commit data into pandas DataFrame
        3. Apply sampling if needed (>threshold)
        4. Generate profile using ydata-profiling
        5. Return HTML or JSON report
        
        Note: Consider memory limits for large commits
        """
        raise NotImplementedError()
```

##### `repository.py` - Explore Repository
```python
class ExploreRepository:
    """
    Repository for exploration jobs using analysis_runs table.
    Replaces in-memory storage with persistent DB storage.
    """
    
    async def create_job(self, conn, commit_id: str, user_id: int, params: Dict) -> int:
        """
        Create exploration job in analysis_runs.
        
        SQL: INSERT INTO analysis_runs 
        Set run_type = 'exploration'
        Store params as JSONB
        """
        raise NotImplementedError()
    
    async def get_job(self, conn, job_id: str) -> Optional[ExploreJob]:
        """
        Retrieve job by ID and map to ExploreJob model.
        
        Implementation Notes:
        1. Query analysis_runs by ID
        2. Map status enum values
        3. Extract params from JSONB
        4. Convert to ExploreJob model
        """
        raise NotImplementedError()
    
    async def update_job_status(
        self, conn, job_id: str, status: JobStatus, **kwargs
    ) -> Optional[ExploreJob]:
        """
        Update job status and optional fields.
        
        SQL: UPDATE analysis_runs SET status = ?, ...
        Map JobStatus enum to analysis_run_status
        """
        raise NotImplementedError()
```

##### `controller.py` - Explore Controller
```python
class ExploreController:
    """Controller for exploration endpoints"""
    
    async def create_exploration_job(
        self, dataset_id: int, version_id: int, request: ExploreRequest, current_user: Any
    ) -> ExploreResponse:
        """
        Create async exploration job.
        
        Implementation Notes:
        1. Validate user permissions
        2. Create job via service
        3. Return job ID and status
        4. Maintain same response format for UI
        """
        raise NotImplementedError()
    
    async def get_job_status(self, job_id: str, current_user: Any) -> JobStatusResponse:
        """
        Get job status and results.
        
        Implementation Notes:
        1. Retrieve from service
        2. Check user permissions
        3. Return status with optional result_url
        4. Keep polling-friendly response format
        """
        raise NotImplementedError()
```

### 6. Sampling Vertical (`app/sampling/`)

#### Files to Keep:
- **`routes.py`** - Sampling endpoints
- **`models.py`** - Complex sampling request models
- **`config.py`** - Sampling configuration

#### Files to Hollow Out:

##### `service.py` - Sampling Service
```python
class SamplingService:
    async def create_multi_round_job(
        self, dataset_id: int, version_id: int, request: MultiRoundSamplingRequest
    ) -> str:
        """
        Create async multi-round sampling job.
        
        Implementation Notes:
        1. Map version_id to commit_id
        2. Create job record in analysis_runs
        3. For each round:
           - Load eligible rows (excluding previous samples)
           - Apply sampling method using PostgreSQL:
             * Random: ORDER BY RANDOM() LIMIT n
             * Stratified: Use PARTITION BY with JSONB field access
             * Systematic: Use ROW_NUMBER() with modulo
             * Weighted: Use JSONB field for weight calculations
           - Store sampled row hashes
           - Track residual for next round
        4. Create merged result commit
        5. Optionally create ref for sample
        
        Returns: Job ID for tracking
        """
        raise NotImplementedError()

    async def get_column_info(self, dataset_id: int, version_id: int) -> Dict:
        """
        Get column metadata for sampling.
        
        Implementation Notes:
        1. Use commit_schemas if available
        2. Otherwise extract from sample of data using PostgreSQL:
           - Use jsonb_object_keys to get all unique keys
           - Use jsonb_typeof to detect types
           - Sample first N rows for efficiency
        3. Include column names, inferred types, nullable info
        
        SQL Example:
        SELECT DISTINCT jsonb_object_keys(data) as column_name
        FROM rows r JOIN commit_rows cr ON r.row_hash = cr.row_hash
        WHERE cr.commit_id = ?
        """
        raise NotImplementedError()
```

##### `repository.py` - Sampling Repository
```python
class SamplingRepository:
    async def create_analysis_run(
        self, conn, commit_id: str, user_id: int, run_type: str, params: Dict
    ) -> int:
        """
        SQL: INSERT INTO analysis_runs
        Track sampling execution
        """
        raise NotImplementedError()

    async def create_sample_commit(
        self, conn, dataset_id: int, parent_commit: str, sample_rows: List[str]
    ) -> str:
        """
        Create derived commit containing sample.
        Message: "Sample from commit {parent[:8]}"
        """
        raise NotImplementedError()
```

##### `controller.py` - Sampling Controller
```python
class SamplingController:
    """Controller for sampling endpoints"""
    
    async def create_multi_round_job(
        self, dataset_id: int, version_id: int, 
        request: MultiRoundSamplingRequest, current_user: Any
    ) -> SamplingJobResponse:
        """
        Create multi-round sampling job.
        
        Implementation Notes:
        1. Validate complex round configurations
        2. Check user permissions
        3. Create job via service
        4. Return job_id for polling
        5. Maintain UI-expected response format
        """
        raise NotImplementedError()
    
    async def get_column_info(
        self, dataset_id: int, version_id: int, current_user: Any
    ) -> ColumnInfoResponse:
        """
        Get columns for sampling configuration.
        
        Implementation Notes:
        1. Call service to extract from commit
        2. Return column names, types, nullable
        3. Keep format for UI dropdown population
        """
        raise NotImplementedError()
```

##### `db_repository.py` - Database-backed Sampling Repository
```python
class SamplingDBRepository:
    """
    Database repository using analysis_runs table.
    Already partially implemented, needs commit-based updates.
    """
    
    async def create_analysis_run(
        self, commit_id: str, user_id: int, request: MultiRoundSamplingRequest
    ) -> int:
        """
        Create sampling job in analysis_runs.
        
        Implementation Updates:
        1. Change dataset_version_id to commit_id reference
        2. Store full request config in run_parameters JSONB
        3. Set run_type = 'sampling'
        4. Return analysis_run ID for tracking
        """
        raise NotImplementedError()
    
    async def update_round_progress(
        self, conn, run_id: int, round_num: int, sample_commit_id: str
    ) -> None:
        """
        Update progress after each sampling round.
        
        SQL: UPDATE analysis_runs 
        SET run_parameters = jsonb_set(run_parameters, 
            '{round_results}', ...)
        WHERE id = ?
        """
        raise NotImplementedError()
```

### 7. Storage Vertical (`app/storage/`)

#### Files to Hollow Out:

##### `backend.py` - Abstract Storage Interface
```python
class StorageBackend(ABC):
    """
    Minimal storage interface for file operations.
    Most storage now happens directly in DB.
    """
    
    async def handle_upload(self, file: UploadFile) -> pd.DataFrame:
        """
        Parse uploaded file to DataFrame.
        Supports CSV, Excel, Parquet formats.
        """
        raise NotImplementedError()

    async def export_dataset(self, commit_id: str, format: str) -> bytes:
        """
        Export commit data to file format.
        Implementation: Load from DB, convert to requested format
        """
        raise NotImplementedError()
```

##### `local_backend.py` - Local Storage Implementation
```python
class LocalStorageBackend(StorageBackend):
    """
    Handles temporary file operations.
    No longer stores actual dataset data.
    """
    
    async def handle_upload(self, file: UploadFile) -> pd.DataFrame:
        """
        Parse uploaded file to DataFrame.
        
        Implementation Notes:
        1. Save to temp directory
        2. Detect format from extension
        3. Parse with pandas/openpyxl/pyarrow
        4. Clean up temp file
        5. Return DataFrame for conversion to rows
        """
        raise NotImplementedError()
```

##### `factory.py` - Storage Factory (Simplified)
```python
class StorageFactory:
    """
    Factory for storage backends.
    Much simpler now - mainly for upload/export operations.
    """
    
    @classmethod
    def create(cls, backend_type: str = "local") -> StorageBackend:
        """
        Create storage backend.
        Currently only local backend needed.
        """
        if backend_type == "local":
            return LocalStorageBackend()
        raise ValueError(f"Unknown backend: {backend_type}")
```

##### `dataset_storage_adapter.py` - TO BE REMOVED
```python
# This file can be completely removed
# All dataset operations now go through repositories
```

### 8. Users Vertical (`app/users/`)

#### Files to Keep:
- **`routes.py`** - Auth endpoints
- **`models.py`** - User models
- **`auth.py`** - JWT handling (minimal changes)

#### Files to Hollow Out:

##### `service.py` - User Service
```python
class UserService:
    async def authenticate_user(self, username: str, password: str) -> Optional[User]:
        """
        Verify credentials and return user.
        
        Implementation Notes:
        1. Query user by soeid
        2. Verify password with bcrypt
        3. Return user with role info
        
        No changes from current implementation
        """
        raise NotImplementedError()

    async def check_dataset_permission(
        self, user_id: int, dataset_id: int, required: str
    ) -> bool:
        """
        Check if user has permission on dataset.
        
        Implementation Notes:
        1. Check dataset_permissions table
        2. Admin role bypasses all checks
        3. Use permission hierarchy
        
        No changes from current implementation
        """
        raise NotImplementedError()
```

##### `controller.py` - User Controller
```python
class UserController:
    """Controller for user/auth endpoints"""
    
    async def login(self, username: str, password: str) -> LoginResponse:
        """
        Handle login request.
        
        Implementation Notes:
        1. Call service.authenticate_user
        2. Generate JWT token
        3. Return token with user info
        4. Keep exact response format for UI
        """
        raise NotImplementedError()
    
    async def get_current_user(self, token: str) -> UserInfo:
        """
        Get current user from token.
        
        Implementation Notes:
        1. Decode JWT
        2. Load user from DB
        3. Return user info with role
        4. Handle token expiry gracefully
        """
        raise NotImplementedError()
```

### 9. Application Entry Points

#### Files to Update:

##### `main.py` - Application Startup
```python
# Key changes needed:
# 1. Update startup event for job recovery:
#    - Query analysis_runs for 'running' status
#    - Reinitialize sampling/explore jobs
#    - Use commit-based references
# 2. Keep all routers and middleware as-is
# 3. Health endpoint can be added if needed

@app.on_event("startup")
async def startup_event():
    """
    Recover running jobs from analysis_runs.
    
    Implementation Notes:
    1. Query for status='running' jobs
    2. Group by run_type (sampling, exploration)
    3. Reinitialize job executors
    4. Log recovery status
    """
    raise NotImplementedError()
```

## Migration Considerations

### Data Migration Strategy
1. **Datasets** → Direct mapping
2. **Files** → Convert to rows with hashing
3. **Dataset versions** → Create commits with proper parent chain
4. **Version files** → Create commit_rows entries
5. **Version tags** → Convert to refs

### Backwards Compatibility
- Keep version number mapping for API compatibility
- Maintain file download endpoints (generate from commits)
- Support overlay format for version creation

### Performance Optimizations
1. **Row deduplication** - Content-addressable storage saves space
2. **Commit indexes** - Fast history traversal
3. **Materialized views** - For common queries
4. **Partitioning** - Consider for large row tables

## Additional Components for Git-like Operations

### Advanced Repository Methods
```python
class DatasetRepository:
    # Git-like operations that leverage the new schema
    async def create_branch(self, conn, dataset_id: int, branch_name: str, from_commit: str):
        """
        Create new branch (ref) pointing to commit.
        SQL: INSERT INTO refs (dataset_id, name, commit_id)
        """
        raise NotImplementedError()
    
    async def get_commit_history(self, conn, commit_id: str, limit: int = 100) -> List[Dict]:
        """
        Get commit history using recursive CTE.
        
        SQL:
        WITH RECURSIVE history AS (
            SELECT * FROM commits WHERE commit_id = ?
            UNION ALL
            SELECT c.* FROM commits c
            JOIN history h ON c.commit_id = h.parent_commit_id
        )
        SELECT * FROM history LIMIT ?
        """
        raise NotImplementedError()
    
    async def calculate_diff(self, conn, commit_a: str, commit_b: str) -> Dict:
        """
        Calculate row differences between commits.
        
        Returns:
        {
            "added": [row_hashes only in B],
            "removed": [row_hashes only in A],
            "total_a": count,
            "total_b": count
        }
        """
        raise NotImplementedError()
```

### Performance Optimizations
```python
class DatasetRepository:
    async def bulk_store_rows(self, conn, rows: List[Dict]) -> List[str]:
        """
        Optimized bulk row storage.
        
        Implementation Notes:
        1. Pre-calculate all hashes
        2. Use COPY or multi-value INSERT
        3. Return: List of row hashes
        
        SQL: 
        INSERT INTO rows (row_hash, data) 
        VALUES (?, ?), (?, ?), ...
        ON CONFLICT DO NOTHING
        """
        raise NotImplementedError()
    
    async def vacuum_orphaned_rows(self, conn) -> int:
        """
        Clean up unreferenced rows.
        
        SQL:
        DELETE FROM rows WHERE row_hash NOT IN (
            SELECT DISTINCT row_hash FROM commit_rows
        )
        
        Returns: Number of rows deleted
        """
        raise NotImplementedError()
```

## Implementation Phases

### Phase 0: Preparation
1. Remove empty health module
2. Remove dataset_storage_adapter.py
3. Document all controller specifications

### Phase 1: Core Infrastructure
1. Update database schema
2. Implement basic commit/row operations
3. Test with simple datasets

### Phase 2: Dataset Operations
1. Implement upload/version creation
2. Add data retrieval
3. Schema management

### Phase 3: Advanced Features
1. Search functionality
2. Sampling operations
3. Statistical analysis

### Phase 4: Git-like Features
1. Branch operations
2. History traversal
3. Diff generation

### Phase 5: Migration & Testing
1. Data migration scripts
2. API compatibility testing
3. Performance validation

## Function Documentation Standards

Each hollowed function should include:
1. **Purpose** - Clear one-line description
2. **Implementation Notes** - Step-by-step algorithm
3. **SQL Queries** - Key queries to implement
4. **Error Handling** - Expected exceptions
5. **Performance Notes** - Optimization hints

## Testing Approach

For each vertical:
1. Keep existing route tests
2. Create integration tests for new implementation
3. Ensure API contracts remain unchanged
4. Add tests for Git-like operations (branching, merging)

## Notes on New Capabilities

The new schema enables:
- **Branching** - Multiple refs per dataset
- **Merging** - Commits with multiple parents
- **History** - Full commit graph traversal
- **Deduplication** - Shared rows between versions
- **Atomicity** - Commits are immutable

## Critical API Compatibility Notes

### Version Number Mapping
Since the UI expects version numbers (1, 2, 3...), we need backwards compatibility:
```python
# In repository layer
async def get_version_number_for_commit(self, conn, commit_id: str) -> int:
    """Count ancestors to generate version number"""
    
async def get_commit_for_version_number(self, conn, dataset_id: int, version: int) -> str:
    """Walk main branch to find Nth commit"""
```

### Response Format Preservation
All API responses must maintain exact same structure:
- Include `version_number` field alongside `commit_id`
- Keep `sheet_name` in responses (even though everything is now one table)
- Maintain same error codes and messages
- Preserve datetime formats

### Controller Layer Importance
Controllers are critical for:
- Maintaining exact API contracts
- Transforming between UI expectations and new storage model
- Handling backwards compatibility
- Preserving error response formats

## Additional Components Missing from Original Plan

### Path Correction Notice

**IMPORTANT**: All file paths in this document show `/app/` for brevity, but the actual implementation uses `/src/app/`. When implementing, use the correct paths:
- `/app/core/` → `/src/app/core/`
- `/app/datasets/` → `/src/app/datasets/`
- etc.

### 1. Alembic Migration Strategy

#### Migration Files Needed:
```python
# alembic/versions/003_git_like_versioning_schema.py
"""
Migration to Git-like versioning system.

Steps:
1. Create new tables (rows, commits, commit_rows, refs)
2. Migrate data from old tables
3. Update search materialized views
4. Drop old tables (after verification)
"""

def upgrade():
    # Create new tables
    op.create_table('rows', ...)
    op.create_table('commits', ...)
    op.create_table('commit_rows', ...)
    op.create_table('refs', ...)
    
    # Migrate existing data
    # - Convert files to rows with hashing
    # - Convert dataset_versions to commits
    # - Create commit_rows from dataset_version_files
    # - Convert version_tags to refs
    
    # Update materialized views
    op.execute("DROP MATERIALIZED VIEW IF EXISTS dataset_search_facets")
    op.execute("""
        CREATE MATERIALIZED VIEW dataset_search_facets AS
        -- Updated query using commits instead of dataset_versions
    """)
    
    # Add indexes for performance
    op.create_index('idx_rows_hash', 'rows', ['row_hash'])
    op.create_index('idx_commits_dataset', 'commits', ['dataset_id'])

def downgrade():
    # Reverse migration for rollback capability
    pass
```

### 2. Background Job Architecture

#### Job Queue Design:
```python
# app/core/jobs.py - Background job management
class JobManager:
    """
    Manages async background jobs without external dependencies.
    Uses asyncio tasks and database for persistence.
    """
    
    async def submit_job(self, job_type: str, params: Dict) -> str:
        """
        Submit job to analysis_runs table.
        Start asyncio task for execution.
        Return job_id for tracking.
        """
        raise NotImplementedError()
    
    async def recover_jobs(self):
        """
        On startup, query analysis_runs for 'running' status.
        Restart interrupted jobs.
        """
        raise NotImplementedError()

# app/core/background_tasks.py
class BackgroundTaskExecutor:
    """
    Executes long-running tasks with proper error handling.
    Updates analysis_runs table with progress.
    """
    
    async def execute_sampling(self, run_id: int, params: Dict):
        """Execute multi-round sampling job"""
        raise NotImplementedError()
    
    async def execute_exploration(self, run_id: int, params: Dict):
        """Execute dataset profiling job"""
        raise NotImplementedError()
    
    async def garbage_collect_rows(self):
        """
        Periodic task to clean orphaned rows.
        Runs based on VERSIONING_BACKGROUND_CONFIG.
        """
        raise NotImplementedError()
```

### 3. Search Infrastructure Updates

#### Materialized View for New Schema:
```sql
-- Updated materialized view for dataset search
CREATE MATERIALIZED VIEW dataset_search_facets AS
WITH latest_commits AS (
    SELECT DISTINCT ON (d.id) 
        d.id as dataset_id,
        d.name,
        d.description,
        d.created_by,
        d.created_at,
        c.commit_id,
        c.committed_at as updated_at,
        cs.row_count,
        cs.size_bytes
    FROM datasets d
    LEFT JOIN refs r ON d.id = r.dataset_id AND r.name = 'main'
    LEFT JOIN commits c ON r.commit_id = c.commit_id
    LEFT JOIN commit_statistics cs ON c.commit_id = cs.commit_id
    ORDER BY d.id, c.committed_at DESC
),
dataset_tags_agg AS (
    SELECT dt.dataset_id, array_agg(t.tag_name) as tags
    FROM dataset_tags dt
    JOIN tags t ON dt.tag_id = t.id
    GROUP BY dt.dataset_id
)
SELECT 
    lc.*,
    u.soeid as creator_name,
    COALESCE(dta.tags, ARRAY[]::text[]) as tags,
    to_tsvector('english', 
        COALESCE(lc.name, '') || ' ' || 
        COALESCE(lc.description, '') || ' ' || 
        COALESCE(array_to_string(dta.tags, ' '), '')
    ) as search_vector
FROM latest_commits lc
LEFT JOIN users u ON lc.created_by = u.id
LEFT JOIN dataset_tags_agg dta ON lc.dataset_id = dta.dataset_id;

CREATE INDEX idx_search_vector ON dataset_search_facets USING gin(search_vector);
```

### 4. Real-time Event System (Optional)

```python
# app/core/events.py - Event broadcasting system
class DatasetEventBroadcaster:
    """
    Broadcasts dataset changes via WebSocket.
    Uses python-socketio for real-time updates.
    """
    
    async def emit_commit_created(self, dataset_id: int, commit_id: str):
        """Notify clients of new commit"""
        raise NotImplementedError()
    
    async def emit_job_status_change(self, job_id: str, status: str):
        """Notify clients of job progress"""
        raise NotImplementedError()
```

### 5. Caching Layer Design

```python
# app/core/cache.py - Optional Redis caching
class CommitCache:
    """
    Cache frequently accessed commits and row data.
    Reduces database load for popular datasets.
    """
    
    async def get_commit_data(self, commit_id: str) -> Optional[List[Dict]]:
        """Get cached commit data if available"""
        raise NotImplementedError()
    
    async def cache_commit_data(self, commit_id: str, data: List[Dict], ttl: int = 3600):
        """Cache commit data with TTL"""
        raise NotImplementedError()
```

### 6. Health Check Enhancement

```python
# In main.py - Enhanced health check
@app.get("/health")
async def health_check():
    """
    Enhanced health check with component status.
    
    Returns:
    {
        "status": "healthy",
        "components": {
            "database": "connected",
            "background_jobs": "running",
            "storage": "available"
        },
        "version": "2.0.0",
        "schema_version": "git-like-v1"
    }
    """
    raise NotImplementedError()
```

### 7. Testing Infrastructure

```python
# tests/conftest.py - Pytest configuration
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

@pytest.fixture
async def test_db():
    """Create test database with new schema"""
    engine = create_async_engine("postgresql+asyncpg://test")
    # Create schema
    # Return connection
    yield conn
    # Cleanup

@pytest.fixture
def mock_commits():
    """Generate test commits with known data"""
    pass

# tests/test_git_operations.py
class TestGitOperations:
    """Test Git-like operations"""
    
    async def test_commit_creation(self):
        """Test creating commits with parent pointers"""
        pass
    
    async def test_branching(self):
        """Test ref creation and updates"""
        pass
    
    async def test_row_deduplication(self):
        """Test content-addressable storage"""
        pass
```

### 8. Monitoring and Metrics

```python
# app/core/metrics.py - Application metrics
class MetricsCollector:
    """
    Collect metrics for monitoring.
    Can integrate with Prometheus/Datadog/etc.
    """
    
    async def record_commit_creation(self, dataset_id: int, row_count: int, duration_ms: float):
        """Track commit creation performance"""
        pass
    
    async def record_query_performance(self, operation: str, duration_ms: float):
        """Track database query performance"""
        pass
```

## Critical Missing Elements Summary

1. **Path References**: All paths need `/src/app/` prefix, not `/app/`
2. **Alembic Migrations**: Complete migration strategy from old to new schema
3. **Background Jobs**: Proper async job management without external dependencies
4. **Search Updates**: Materialized views need updating for new schema

## Recommended Implementation Order

1. **Phase 0.5: Infrastructure Updates**
   - Fix path references throughout plan
   - Setup testing infrastructure
   - Design background job system

2. **Update Phase 1**: Include Alembic migrations
3. **Update Phase 3**: Include search materialized view updates
4. **New Phase 6**: Background Jobs & Monitoring
5. **New Phase 7**: Performance Optimization (caching, metrics)

This ensures a complete, production-ready implementation of the Git-like versioning system.