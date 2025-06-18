# Integrated System Architecture - Versioning and Visualization

## Table of Contents
1. [System Overview](#system-overview)
2. [Integration Philosophy](#integration-philosophy)
3. [End-to-End User Workflows](#end-to-end-user-workflows)
4. [Technical Integration Points](#technical-integration-points)
5. [Design Principles](#design-principles)
6. [Performance and Scalability](#performance-and-scalability)
7. [Security Architecture](#security-architecture)
8. [Operational Considerations](#operational-considerations)

## System Overview

### The Complete Data Platform

Our system combines two powerful subsystems:

1. **Dataset Versioning System**: Git-like version control for datasets
2. **Perspective Visualization**: High-performance interactive analytics

Together, they provide a complete solution for:
- Collaborative data management
- Version-controlled analytics
- Interactive exploration
- Reproducible analysis

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                       User Interface                         │
│                  (Web App / API Clients)                     │
└─────────────────┬───────────────────────┬───────────────────┘
                  │                       │
         ┌────────▼──────────┐   ┌───────▼────────────┐
         │   Versioning API  │   │  Perspective API   │
         │                   │   │                    │
         │ • Upload/Download │   │ • Create Tables    │
         │ • Branch/Tag      │   │ • Create Views     │
         │ • History/Diff    │   │ • Query Data       │
         └────────┬──────────┘   └───────┬────────────┘
                  │                       │
         ┌────────▼──────────────────────▼────────────┐
         │          Service Layer                     │
         │                                            │
         │ • Permission Checks                        │
         │ • Data Validation                          │
         │ • Format Conversion                        │
         │ • Cache Management                         │
         └────────┬───────────────────────────────────┘
                  │
         ┌────────▼───────────────────────────────────┐
         │          Data Layer                        │
         │                                            │
         │ ┌─────────────┐      ┌─────────────────┐ │
         │ │  PostgreSQL │      │ Perspective     │ │
         │ │             │      │ Memory Tables   │ │
         │ │ • Metadata  │      │                 │ │
         │ │ • Versions  │      │ • Active Tables │ │
         │ │ • Pointers  │      │ • Views         │ │
         │ └─────────────┘      └─────────────────┘ │
         │                                            │
         │ ┌─────────────────────────────────────┐  │
         │ │         File Storage                │  │
         │ │   /data/datasets/{id}/{version}/    │  │
         │ │        Parquet Files                │  │
         │ └─────────────────────────────────────┘  │
         └────────────────────────────────────────────┘
```

## Integration Philosophy

### 1. Separation of Concerns

**Versioning System**:
- Handles persistence and history
- Manages branches and tags
- Ensures data integrity
- Controls access permissions

**Perspective System**:
- Handles interactive analysis
- Manages in-memory performance
- Provides visualization capabilities
- Enables real-time aggregations

### 2. Loose Coupling

The systems are integrated but not interdependent:

```python
# Versioning can work without Perspective
dataset = upload_dataset(file, metadata)
version = create_version(dataset_id, new_file)

# Perspective can work with any data source
table = create_perspective_table(data_source)
view = create_view(table, config)

# But they work better together
version = get_dataset_version(dataset_id, version_id)
table = load_into_perspective(version)
```

### 3. Consistent Abstractions

Both systems use similar concepts:
- **Immutability**: Versions and tables are immutable once created
- **Pointer-based References**: Branches/tags and table IDs
- **Metadata-First**: Schema and properties before data
- **Progressive Operations**: Lazy loading and streaming

## End-to-End User Workflows

### Workflow 1: Data Science Experimentation

```
1. Data Scientist uploads initial dataset
   → Creates version 1 on 'main' branch
   → Auto-converts to Parquet format
   → Captures schema automatically

2. Creates experimental branch
   → POST /api/datasets/{id}/branches
   → Branch 'experiment-normalization' → version 1

3. Loads data into Perspective
   → POST /api/explore/{id}/{version}/perspective
   → Returns table_id for interactive analysis

4. Explores data interactively
   → Creates pivots and aggregations
   → Identifies data quality issues
   → Exports cleaned subset

5. Uploads cleaned data to branch
   → POST /api/datasets/{id}/branches/experiment-normalization/commit
   → Creates version 2 with parent = 1
   → Includes commit message explaining changes

6. Compares schemas
   → POST /api/datasets/{id}/schema/compare
   → Shows normalized columns added
   → Validates no data loss

7. Merges to main (manual process)
   → Downloads branch version
   → Uploads to main branch
   → Tags as 'v1.1-normalized'
```

### Workflow 2: Production Reporting

```
1. Analyst accesses production dataset
   → GET /api/datasets/{id}/pointers/v2.0-release
   → Gets immutable tagged version

2. Loads specific version into Perspective
   → POST /api/explore/{id}/{version}/perspective
   → Applies row limit for performance
   → Selects only needed columns

3. Creates standard report view
   → POST /api/explore/perspective/table/{table_id}/view
   → Configures monthly aggregation
   → Groups by region and product

4. Saves view configuration
   → Stores in user preferences
   → Can be recreated on any version

5. Schedules automated export
   → System recreates view weekly
   → Exports to reporting system
   → Always uses tagged version
```

### Workflow 3: Collaborative Analysis

```
1. Team A uploads customer data
   → Creates dataset with 'customer-analytics' tag
   → Grants read access to Team B

2. Team B creates analysis branch
   → POST /api/datasets/{id}/branches
   → Branch 'q4-campaign-analysis' → latest

3. Multiple analysts load same data
   → All POST to /api/explore/{id}/{version}/perspective
   → System returns same table_id (shared)
   → Each creates different views

4. Analyst 1 creates demographic pivot
   → Groups by age and region
   → Saves view configuration

5. Analyst 2 creates behavior analysis
   → Groups by product and frequency
   → References same underlying table

6. Results combined and uploaded
   → New version with combined insights
   → Tagged as 'q4-analysis-final'
   → Original data unchanged
```

## Technical Integration Points

### 1. Data Flow Integration

```python
# Versioning → Perspective Pipeline
async def load_version_to_perspective(dataset_id: int, version_id: int):
    # 1. Get version metadata
    version = await dataset_service.get_version(dataset_id, version_id)
    
    # 2. Check permissions
    if not has_permission(current_user, dataset_id, "read"):
        raise PermissionError()
    
    # 3. Get file path
    file_path = version.primary_file.path
    
    # 4. Load with DuckDB (efficient for Parquet)
    arrow_table = duckdb.query(
        f"SELECT * FROM '{file_path}'"
    ).arrow()
    
    # 5. Create Perspective table
    table_id = f"dataset_{dataset_id}_v{version_id}_{hash}"
    perspective_table = perspective.Table(arrow_table)
    
    # 6. Store in manager
    perspective_manager.store_table(table_id, perspective_table)
    
    return table_id
```

### 2. Schema Integration

```python
# Use versioning schema for Perspective validation
async def validate_perspective_view(table_id: str, view_config: dict):
    # 1. Extract dataset/version from table_id
    dataset_id, version_id = parse_table_id(table_id)
    
    # 2. Get schema from versioning system
    schema = await dataset_service.get_version_schema(version_id)
    
    # 3. Validate view configuration
    for column in view_config.get("group_by", []):
        if column not in schema.columns:
            raise ValueError(f"Column {column} not in schema")
    
    # 4. Check aggregation compatibility
    for col, agg in view_config.get("aggregates", {}).items():
        col_type = schema.get_column_type(col)
        if not is_aggregation_valid(col_type, agg):
            raise ValueError(f"Cannot {agg} on {col_type} column")
```

### 3. Permission Integration

```python
# Unified permission checking
async def check_data_access(user_id: int, dataset_id: int, operation: str):
    # 1. Check dataset-level permissions
    has_access = await permission_service.has_permission(
        user_id, "dataset", dataset_id, operation
    )
    
    # 2. Check row-level security (future)
    if operation == "read" and has_row_security(dataset_id):
        filters = await get_user_filters(user_id, dataset_id)
        return has_access, filters
    
    return has_access, None

# Apply to both systems
async def perspective_create_table(dataset_id: int, version_id: int, user_id: int):
    # Check permission with versioning system
    has_access, filters = await check_data_access(user_id, dataset_id, "read")
    
    if not has_access:
        raise PermissionError()
    
    # Load with filters if applicable
    return await load_version_to_perspective(
        dataset_id, version_id, filters=filters
    )
```

### 4. Lifecycle Integration

```python
# Coordinate cleanup between systems
async def cleanup_dataset_resources(dataset_id: int):
    # 1. Find all Perspective tables for this dataset
    table_pattern = f"dataset_{dataset_id}_v*"
    tables = perspective_manager.find_tables(table_pattern)
    
    # 2. Check active connections
    for table_id in tables:
        if perspective_manager.has_active_connections(table_id):
            logger.warning(f"Cannot cleanup {table_id}: active connections")
            continue
        
        # 3. Remove from Perspective
        perspective_manager.remove_table(table_id)
    
    # 4. Clean up versioning system
    await dataset_service.cleanup_old_versions(dataset_id)
```

## Design Principles

### 1. Immutability First

**Why**: Ensures reproducibility and audit trails

**Implementation**:
- Dataset versions are immutable after creation
- Perspective tables are read-only
- Views are computed, not stored
- Tags cannot be moved

### 2. Lazy Loading

**Why**: Optimizes resource usage and response times

**Implementation**:
- Metadata returned immediately
- Data loaded on demand
- Schemas cached aggressively
- Large results paginated

### 3. Fail-Safe Defaults

**Why**: Prevents system overload and data loss

**Implementation**:
- Row limits on Perspective tables
- Timeout on long operations
- Automatic branch creation
- Permission denial by default

### 4. Progressive Enhancement

**Why**: System remains useful even with failures

**Implementation**:
- Versioning works without Perspective
- Basic download works without conversion
- Schema capture continues on parse errors
- Partial results returned when possible

## Performance and Scalability

### Bottleneck Analysis

1. **File Storage I/O**:
   - Bottleneck: Reading large Parquet files
   - Mitigation: SSD storage, read caching, column pruning

2. **Memory Usage**:
   - Bottleneck: Multiple large Perspective tables
   - Mitigation: LRU eviction, table size limits, shared tables

3. **Database Queries**:
   - Bottleneck: Version tree traversal
   - Mitigation: Indexed parent_id, materialized paths (future)

4. **Network Transfer**:
   - Bottleneck: Large dataset downloads
   - Mitigation: Compression, streaming, pagination

### Scaling Strategies

#### Vertical Scaling
```yaml
# Resource allocation by component
perspective_server:
  memory: 64GB  # For in-memory tables
  cpu: 16 cores # For parallel aggregations

api_server:
  memory: 8GB   # For request handling
  cpu: 8 cores  # For concurrent requests

database:
  memory: 16GB  # For query caching
  cpu: 8 cores  # For complex queries
  storage: SSD  # For fast metadata access

file_storage:
  type: NVMe SSD       # For fast Parquet reads
  capacity: 10TB       # For version storage
  iops: 100000         # For concurrent access
```

#### Horizontal Scaling
```
                Load Balancer
                     │
        ┌────────────┼────────────┐
        │            │            │
   API Server 1  API Server 2  API Server 3
        │            │            │
        └────────────┼────────────┘
                     │
              Shared Services
                     │
   ┌─────────────────┼─────────────────┐
   │                 │                 │
PostgreSQL    Redis Cache    Perspective Pool
(Primary)     (Metadata)    (Server 1, 2, 3)
   │
Read Replicas
```

### Caching Hierarchy

1. **Browser Cache**: Static assets, view configurations
2. **CDN Cache**: Dataset metadata, schemas
3. **Redis Cache**: Table metadata, user sessions
4. **Application Cache**: Parsed schemas, permissions
5. **Perspective Cache**: Computed views, aggregations

## Security Architecture

### Defense in Depth

```
Layer 1: Network Security
├── HTTPS only
├── API rate limiting  
└── DDoS protection

Layer 2: Authentication
├── JWT tokens
├── Session management
└── MFA support

Layer 3: Authorization  
├── Dataset permissions
├── Row-level security
└── Column masking

Layer 4: Data Security
├── Encryption at rest
├── Encrypted transport
└── Audit logging

Layer 5: Application Security
├── Input validation
├── SQL injection prevention
└── Path traversal protection
```

### Data Access Control

```python
# Fine-grained permission model
class DataPermission:
    dataset_id: int
    user_id: int
    permission_type: str  # read, write, admin
    row_filter: Optional[str]  # SQL WHERE clause
    column_mask: Optional[List[str]]  # Hidden columns
    time_bound: Optional[DateRange]  # Temporal access
    
# Applied at every layer
def apply_permissions(query: Query, permissions: DataPermission):
    if permissions.row_filter:
        query = query.where(permissions.row_filter)
    
    if permissions.column_mask:
        visible_columns = [c for c in query.columns 
                          if c not in permissions.column_mask]
        query = query.select(visible_columns)
    
    return query
```

## Operational Considerations

### Monitoring and Alerting

```yaml
# Key metrics to monitor
metrics:
  versioning:
    - dataset_upload_duration
    - version_creation_rate
    - storage_usage_by_dataset
    - schema_change_frequency
    
  perspective:
    - table_creation_time
    - memory_usage_per_table
    - active_table_count
    - view_computation_time
    
  integration:
    - end_to_end_latency
    - permission_check_duration
    - cache_hit_ratio
    - error_rate_by_endpoint

alerts:
  - name: high_memory_usage
    condition: perspective_memory_usage > 80%
    action: evict_idle_tables
    
  - name: slow_version_creation
    condition: version_creation_p95 > 30s
    action: investigate_file_size
    
  - name: permission_denial_spike
    condition: permission_denied_rate > 10/min
    action: check_for_attack
```

### Backup and Recovery

```yaml
backup_strategy:
  # Metadata - Critical
  postgresql:
    frequency: hourly
    retention: 30 days
    type: incremental
    
  # File data - Large
  parquet_files:
    frequency: daily
    retention: 90 days
    type: incremental
    location: s3://backup-bucket
    
  # Perspective tables - Ephemeral
  memory_tables:
    frequency: none  # Regenerated from files
    
recovery_plan:
  rto: 4 hours  # Recovery time objective
  rpo: 1 hour   # Recovery point objective
  
  steps:
    1: Restore PostgreSQL metadata
    2: Restore file storage
    3: Rebuild search indexes
    4: Warm cache layer
    5: Recreate active Perspective tables
```

### Maintenance Windows

```python
# Graceful maintenance mode
async def enter_maintenance_mode():
    # 1. Stop accepting new requests
    await api_server.stop_accepting()
    
    # 2. Wait for in-flight requests
    await api_server.drain_connections(timeout=300)
    
    # 3. Save Perspective table list
    active_tables = await perspective_manager.list_active_tables()
    await cache.set("maintenance:active_tables", active_tables)
    
    # 4. Gracefully shutdown Perspective
    await perspective_manager.shutdown()
    
    # 5. Perform maintenance
    yield  # Maintenance happens here
    
    # 6. Restore state
    await perspective_manager.start()
    for table_info in active_tables:
        await perspective_manager.preload_table(table_info)
```

## Conclusion

The integrated system provides a powerful platform for versioned data analysis by combining the strengths of Git-like version control with high-performance interactive analytics. The architecture supports everything from individual data exploration to enterprise-scale collaborative analysis, while maintaining security, performance, and reliability.

Key success factors:
1. Clear separation of concerns between versioning and visualization
2. Consistent abstractions across subsystems
3. Progressive enhancement and graceful degradation
4. Comprehensive security at every layer
5. Operational excellence through monitoring and automation

The system is designed to grow with organizational needs while maintaining the simplicity and performance that makes it valuable for end users.