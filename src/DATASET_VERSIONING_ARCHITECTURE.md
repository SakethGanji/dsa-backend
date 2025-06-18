# Dataset Versioning System - Architecture and Design Decisions

## Table of Contents
1. [Design Philosophy](#design-philosophy)
2. [Database Schema Deep Dive](#database-schema-deep-dive)
3. [Architectural Decisions and Reasoning](#architectural-decisions-and-reasoning)
4. [Performance Considerations](#performance-considerations)
5. [Security and Permissions](#security-and-permissions)
6. [Future Roadmap and Scalability](#future-roadmap-and-scalability)

## Design Philosophy

### Why Git-like Versioning for Datasets?

The decision to implement a Git-like versioning system for datasets was driven by several key requirements:

1. **Familiar Mental Model**: Data scientists and analysts are already familiar with Git concepts (branches, tags, commits). This reduces the learning curve and makes the system intuitive.

2. **Collaborative Workflows**: Just like code, datasets often require collaborative editing, experimentation, and review processes. Teams need to work on different versions simultaneously without conflicts.

3. **Audit Trail and Compliance**: Many organizations require complete audit trails for data changes. A versioning system provides immutable history and clear lineage.

4. **Experimentation Safety**: Users can create experimental branches without affecting production data, similar to feature branches in code development.

5. **Rollback Capability**: When data quality issues are discovered, teams need to quickly revert to known-good versions.

## Database Schema Deep Dive

### Core Design Principles

1. **Immutability**: Versions are never modified after creation
2. **Referential Integrity**: All relationships enforced at database level
3. **Efficient Querying**: Indexes on commonly queried fields
4. **Extensibility**: JSONB fields for flexible metadata

### 1. `datasets` Table - The Root Entity

```sql
CREATE TABLE datasets (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_by INTEGER REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_deleted BOOLEAN DEFAULT FALSE,
    tags TEXT[]  -- PostgreSQL array for efficient tag queries
);
```

**Design Reasoning:**
- **Minimal Core**: Only essential metadata stored here. Version-specific data lives in `dataset_versions`.
- **Soft Deletes**: `is_deleted` flag preserves referential integrity while allowing logical deletion.
- **Array Tags**: PostgreSQL arrays allow efficient "contains" queries without join tables for simple tagging.

### 2. `dataset_versions` Table - The Version History

```sql
CREATE TABLE dataset_versions (
    id SERIAL PRIMARY KEY,
    dataset_id INTEGER REFERENCES datasets(id),
    version_number INTEGER NOT NULL,
    file_id INTEGER REFERENCES files(id),
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_updated_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    uploaded_by INTEGER REFERENCES users(id),
    parent_version_id INTEGER REFERENCES dataset_versions(id),
    message TEXT,
    overlay_file_id INTEGER REFERENCES files(id),
    UNIQUE(dataset_id, version_number)
);
```

**Design Reasoning:**

- **version_number**: Sequential per dataset, not global. This makes version numbers meaningful (v1, v2, v3) rather than arbitrary IDs.

- **parent_version_id**: Creates the DAG structure. NULL for initial versions. This enables:
  - Branch history traversal
  - Merge ancestry (future feature)
  - Fork detection

- **file_id**: Legacy single-file support. Maintained for backward compatibility.

- **message**: Commit-style messages for context. Critical for understanding why changes were made.

- **overlay_file_id**: Supports incremental updates (future feature) where only changes are stored.

**Why Not Use Git Directly?**
- Git is optimized for text diffs, not binary data
- No efficient columnar queries
- Difficult to integrate with existing data infrastructure
- Limited access control at row/column level

### 3. `dataset_pointers` Table - Branches and Tags

```sql
CREATE TABLE dataset_pointers (
    id SERIAL PRIMARY KEY,
    dataset_id INTEGER REFERENCES datasets(id),
    pointer_name VARCHAR(255) NOT NULL,
    dataset_version_id INTEGER REFERENCES dataset_versions(id),
    is_tag BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(dataset_id, pointer_name)
);
```

**Design Reasoning:**

- **Unified Table**: Both branches and tags in one table simplifies queries and enforces uniqueness.

- **is_tag Flag**: Simple boolean differentiates mutable branches from immutable tags. Alternative designs considered:
  - Separate tables: More complex queries, duplicate logic
  - Enum type: Less flexible for future pointer types

- **pointer_name**: Human-readable references. "main", "dev", "v1.0-release" are more meaningful than version IDs.

**Branch vs Tag Philosophy:**
- **Branches**: Mutable pointers for active development (main, feature-xyz)
- **Tags**: Immutable markers for milestones (v1.0, quarterly-snapshot-2024Q1)

### 4. `dataset_version_files` Table - Multi-file Support

```sql
CREATE TABLE dataset_version_files (
    version_id INTEGER REFERENCES dataset_versions(id),
    file_id INTEGER REFERENCES files(id),
    component_type VARCHAR(50) NOT NULL,
    component_name VARCHAR(255),
    component_index INTEGER DEFAULT 0,
    metadata JSONB,
    PRIMARY KEY (version_id, file_id, component_type, COALESCE(component_name, ''))
);
```

**Design Reasoning:**

- **Composite Primary Key**: Ensures uniqueness while allowing multiple files per version.

- **component_type**: Categorizes files (primary, metadata, schema, documentation). Enables:
  - Type-specific processing
  - UI organization
  - Validation rules per type

- **component_name**: Optional human-readable identifier within type.

- **metadata JSONB**: Flexible storage for component-specific data:
  ```json
  {
    "description": "Customer demographics supplement",
    "source_system": "CRM",
    "processing_notes": "Anonymized PII fields"
  }
  ```

**Use Cases:**
- Excel with multiple sheets
- Dataset with separate metadata JSON
- Documentation files attached to data
- Schema evolution tracking files

### 5. `files` Table - Physical Storage

```sql
CREATE TABLE files (
    id SERIAL PRIMARY KEY,
    storage_type VARCHAR(50) DEFAULT 'filesystem',
    file_type VARCHAR(50),
    file_path TEXT,
    file_size BIGINT,
    content_hash VARCHAR(64),
    reference_count INTEGER DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

**Design Reasoning:**

- **storage_type**: Abstracts storage backend. Supports:
  - 'filesystem': Local disk
  - 's3': Amazon S3 (future)
  - 'azure': Azure Blob (future)
  - 'gcs': Google Cloud Storage (future)

- **content_hash**: SHA-256 hash for:
  - Deduplication (same file used by multiple versions)
  - Integrity verification
  - Content-based addressing (future)

- **reference_count**: Tracks usage for garbage collection. When zero, file can be deleted.

**Storage Strategy:**
```
/data/datasets/{dataset_id}/{version_id}/
├── data.parquet          # Standardized format
├── original.csv          # Keep original for fidelity
└── metadata.json         # Processing metadata
```

### 6. `dataset_schema_versions` Table - Schema Evolution

```sql
CREATE TABLE dataset_schema_versions (
    id SERIAL PRIMARY KEY,
    dataset_version_id INTEGER REFERENCES dataset_versions(id),
    schema_json JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

**Design Reasoning:**

- **Automatic Capture**: Schema extracted on every upload, not user-managed.

- **JSONB Format**: Flexible schema representation:
  ```json
  {
    "columns": [
      {
        "name": "customer_id",
        "type": "integer",
        "nullable": false,
        "primary_key": true
      },
      {
        "name": "purchase_date", 
        "type": "timestamp",
        "format": "YYYY-MM-DD HH:mm:ss"
      }
    ],
    "row_count": 50000,
    "source_format": "csv",
    "encoding": "utf-8"
  }
  ```

- **Schema Comparison**: Enables diff operations between versions.

## Architectural Decisions and Reasoning

### 1. Why Parquet as Internal Format?

**Decision**: All uploaded files are converted to Parquet format.

**Reasoning:**
- **Columnar Storage**: 10-100x compression for analytical queries
- **Schema Preservation**: Self-describing format with metadata
- **Query Performance**: Native support in DuckDB, Spark, etc.
- **Industry Standard**: Wide ecosystem support

**Trade-offs:**
- Conversion overhead on upload
- Not ideal for row-based operations
- Loses some source format fidelity

### 2. Version Number Strategy

**Decision**: Sequential integers per dataset, not global.

**Reasoning:**
- **User-Friendly**: "Version 3" is clearer than "Version 7fa2eb"
- **Ordering**: Natural sort order for UI display
- **Simplicity**: No need for complex ID generation

**Alternative Considered**: Content-based hashes (like Git)
- Pros: Deduplication, integrity
- Cons: Not user-friendly, complex implementation

### 3. Branch Creation Philosophy

**Decision**: Branches are lightweight pointers, not copies.

**Reasoning:**
- **Storage Efficiency**: No data duplication on branch creation
- **Instant Operations**: Creating a branch is just an INSERT
- **Flexible Workflows**: Easy to create experimental branches

**Implication**: First commit to branch creates new version with parent pointer.

### 4. Parent-Child Relationship Model

**Decision**: Single parent per version (no merge commits yet).

**Reasoning:**
- **Simplicity**: Easier to implement and understand
- **Clear Lineage**: Unambiguous history
- **Sufficient for v1**: Most data workflows are linear

**Future Enhancement**: Multiple parents for merge operations.

### 5. File Storage Architecture

**Decision**: Each version gets complete file copy (no deltas).

**Reasoning for v1:**
- **Simplicity**: No complex delta calculation
- **Reliability**: Each version is self-contained
- **Performance**: No reconstruction overhead

**Future Optimization:**
- Content-addressable storage with deduplication
- Delta compression for similar versions
- Tiered storage (hot/cold)

## Performance Considerations

### Query Optimization

1. **Indexes Created:**
   ```sql
   CREATE INDEX idx_dataset_versions_dataset_id ON dataset_versions(dataset_id);
   CREATE INDEX idx_dataset_pointers_dataset_id ON dataset_pointers(dataset_id);
   CREATE INDEX idx_version_files_version_id ON dataset_version_files(version_id);
   ```

2. **Common Query Patterns Optimized:**
   - List versions of dataset: O(log n) with index
   - Get branch HEAD: O(1) with unique constraint
   - Traverse history: O(depth) following parent pointers

### Memory Management

1. **Streaming Uploads**: Files processed in chunks, not loaded entirely in memory
2. **Lazy Loading**: Metadata loaded separately from file data
3. **Connection Pooling**: Database connections reused

### Caching Strategy

1. **Pointer Cache**: Branch/tag lookups cached in memory (5-minute TTL)
2. **Schema Cache**: Parsed schemas cached to avoid repeated JSON parsing
3. **File Metadata**: Size and hash cached to avoid filesystem calls

## Security and Permissions

### Permission Model Integration

```python
class DatasetPermissionType(Enum):
    READ = "read"      # View dataset and versions
    WRITE = "write"    # Upload new versions, create branches
    ADMIN = "admin"    # Delete, grant permissions
```

### Security Decisions

1. **Row-Level Security**: Each operation checks user permissions
2. **Audit Logging**: All modifications logged with user ID
3. **No Direct File Access**: All file operations go through API
4. **Path Traversal Protection**: File paths sanitized and validated

## Future Roadmap and Scalability

### Phase 1: Current Implementation ✓
- Basic versioning with linear history
- Branch and tag support  
- Multi-file versions
- Schema tracking

### Phase 2: Storage Optimization
- **Content-Addressable Storage**: Deduplication based on file hash
- **Delta Compression**: Store only differences between versions
- **Tiered Storage**: Move old versions to cheaper storage

### Phase 3: Advanced Features
- **Merge Operations**: Combine changes from multiple branches
- **Conflict Resolution**: Handle overlapping changes
- **Incremental Updates**: Append-only operations for time series
- **Partitioned Datasets**: Version subsets independently

### Phase 4: Enterprise Features
- **Distributed Storage**: Multi-region replication
- **Streaming Integration**: Version streams in real-time
- **Lineage Tracking**: Full data provenance
- **Policy-Based Retention**: Automatic cleanup rules

### Scalability Considerations

1. **Horizontal Scaling**:
   - Stateless API servers
   - Read replicas for queries
   - Sharded storage by dataset ID

2. **Large Dataset Handling**:
   - Chunked uploads
   - Partial version loading
   - Metadata-only operations

3. **High Version Count**:
   - Pagination for version lists
   - Archived version tables
   - Summary statistics caching

## Conclusion

The dataset versioning system provides a robust foundation for collaborative data management. By borrowing proven concepts from Git while adapting them for data-specific needs, we've created an intuitive yet powerful system. The architecture balances current simplicity with future extensibility, ensuring the system can grow with organizational needs while maintaining performance and reliability.