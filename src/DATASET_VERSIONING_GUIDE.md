# Dataset Versioning System - Complete Documentation

## Table of Contents
1. [Overview](#overview)
2. [Database Schema](#database-schema)
3. [Core Concepts](#core-concepts)
4. [API Endpoints Reference](#api-endpoints-reference)
5. [Detailed Database Flow](#detailed-database-flow)
6. [Workflow Examples](#workflow-examples)
7. [Implementation Details](#implementation-details)

## Overview

The dataset versioning system implements a Git-like version control system for datasets. It supports:
- **Branches**: Mutable pointers to versions (like Git branches)
- **Tags**: Immutable pointers to versions (like Git tags)
- **Parent-Child Relationships**: Versions can have parents, creating a DAG structure
- **Multi-file Support**: Versions can contain multiple files
- **Schema Tracking**: Automatic schema capture and comparison

## Database Schema

### Core Tables

#### 1. `dataset_versions`
Stores each version of a dataset.

```sql
CREATE TABLE dataset_versions (
    id SERIAL PRIMARY KEY,
    dataset_id INTEGER REFERENCES datasets(id),
    version_number INTEGER NOT NULL,
    file_id INTEGER REFERENCES files(id),  -- Legacy single file
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_updated_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    uploaded_by INTEGER REFERENCES users(id),
    parent_version_id INTEGER REFERENCES dataset_versions(id),  -- For branching
    message TEXT,  -- Commit message
    overlay_file_id INTEGER REFERENCES files(id),
    UNIQUE(dataset_id, version_number)
);
```

#### 2. `dataset_pointers`
Manages branches and tags.

```sql
CREATE TABLE dataset_pointers (
    id SERIAL PRIMARY KEY,
    dataset_id INTEGER REFERENCES datasets(id),
    pointer_name VARCHAR(255) NOT NULL,
    dataset_version_id INTEGER REFERENCES dataset_versions(id),
    is_tag BOOLEAN DEFAULT FALSE,  -- false = branch, true = tag
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(dataset_id, pointer_name)
);
```

#### 3. `dataset_version_files`
Associates multiple files with versions.

```sql
CREATE TABLE dataset_version_files (
    version_id INTEGER REFERENCES dataset_versions(id),
    file_id INTEGER REFERENCES files(id),
    component_type VARCHAR(50) NOT NULL,  -- 'primary', 'metadata', 'schema', etc.
    component_name VARCHAR(255),
    component_index INTEGER DEFAULT 0,
    metadata JSONB,
    PRIMARY KEY (version_id, file_id, component_type, COALESCE(component_name, ''))
);
```

#### 4. `dataset_schema_versions`
Tracks schema evolution.

```sql
CREATE TABLE dataset_schema_versions (
    id SERIAL PRIMARY KEY,
    dataset_version_id INTEGER REFERENCES dataset_versions(id),
    schema_json JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

#### 5. `files`
Stores file metadata and paths.

```sql
CREATE TABLE files (
    id SERIAL PRIMARY KEY,
    storage_type VARCHAR(50) DEFAULT 'filesystem',
    file_type VARCHAR(50),
    file_path TEXT,
    file_size BIGINT,
    content_hash VARCHAR(64),  -- For future deduplication
    reference_count INTEGER DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

## Core Concepts

### Version Numbers
- Each dataset has sequential version numbers (1, 2, 3, ...)
- Version numbers are unique per dataset
- Automatically incremented on each new version

### Branches
- Mutable pointers to versions
- Default branch: "main" (created on first upload)
- Can be updated to point to different versions
- Support branching from any version

### Tags
- Immutable pointers to versions
- Cannot be changed once created
- Used for marking releases or important versions

### Parent-Child Relationships
- Each version can have a parent version
- Creates a directed acyclic graph (DAG)
- Enables branch history and version trees

### File Storage
- Files stored at: `/data/datasets/{dataset_id}/{version_id}/`
- All files converted to Parquet format
- Each version gets its own copy (no deduplication yet)

## API Endpoints Reference

### Core Version Management

#### 1. Upload Dataset / Create Version
```http
POST /api/datasets/upload
Content-Type: multipart/form-data

Parameters:
- file: File (required) - The dataset file
- dataset_id: int (optional) - For creating new version
- name: string (required) - Dataset name
- description: string (optional)
- tags: string (optional) - JSON array or comma-separated
- parent_version_id: int (optional) - For branching
- message: string (optional) - Version message
- branch_name: string (default: "main") - Target branch

Response: DatasetUploadResponse
{
    "dataset_id": 123,
    "version_id": 456,
    "version_number": 2,
    "branch_name": "main"
}
```

**Flow:**
1. If no dataset_id, creates new dataset with version 1
2. Creates "main" branch pointing to version 1
3. If dataset_id provided, creates new version
4. Updates branch pointer to new version
5. Stores file in `/data/datasets/{dataset_id}/{version_id}/`
6. Captures schema automatically

#### 2. List Dataset Versions
```http
GET /api/datasets/{dataset_id}/versions

Response: List[DatasetVersion]
[
    {
        "id": 456,
        "dataset_id": 123,
        "version_number": 2,
        "parent_version_id": 455,
        "message": "Updated data",
        "ingestion_timestamp": "2024-01-01T00:00:00Z",
        "uploaded_by": 1
    }
]
```

#### 3. Get Specific Version
```http
GET /api/datasets/{dataset_id}/versions/{version_id}

Response: DatasetVersion with full details
```

#### 4. Download Version File
```http
GET /api/datasets/{dataset_id}/versions/{version_id}/download

Response: File stream (Parquet format)
```

#### 5. Delete Version
```http
DELETE /api/datasets/{dataset_id}/versions/{version_id}

Requirements: Admin permission
Response: 204 No Content
```

**Note:** Deleting a version that branches depend on may break history.

#### 6. Get Version Tree
```http
GET /api/datasets/{dataset_id}/versions/tree

Response: Version DAG structure
{
    "root_versions": [1],
    "tree": {
        "1": {
            "version": {...},
            "children": [2, 3]
        },
        "2": {
            "version": {...},
            "children": [4]
        }
    }
}
```

### Branch Operations

#### 7. Create Branch
```http
POST /api/datasets/{dataset_id}/branches
Content-Type: application/json

{
    "branch_name": "feature-branch",
    "from_version_id": 2
}

Response: DatasetPointer
{
    "id": 789,
    "dataset_id": 123,
    "pointer_name": "feature-branch",
    "dataset_version_id": 2,
    "is_tag": false
}
```

**SQL Operations:**
1. Inserts into `dataset_pointers` table
2. Sets `is_tag = false` for branch
3. Points to specified version

#### 8. Commit to Branch
```http
POST /api/datasets/{dataset_id}/branches/{branch_name}/commit
Content-Type: multipart/form-data

Parameters:
- file: File (required)
- message: string (optional)

Response: DatasetUploadResponse
```

**Flow:**
1. Gets current branch pointer from `dataset_pointers`
2. Creates new version with parent = branch's current version
3. Updates branch pointer to new version
4. Stores file and captures schema

#### 9. Update Branch Pointer
```http
PATCH /api/datasets/{dataset_id}/branches/{branch_name}
Content-Type: application/json

{
    "to_version_id": 5
}

Response: Success message
```

**Use Case:** Moving branch to different version (like git reset)

#### 10. Get Branch HEAD
```http
GET /api/datasets/{dataset_id}/branches/{branch_name}/head

Response: Current version on branch
```

#### 11. Get Branch History
```http
GET /api/datasets/{dataset_id}/branches/{branch_name}/history

Response: List of versions following parent links
```

### Tag Operations

#### 12. Create Tag
```http
POST /api/datasets/{dataset_id}/tags
Content-Type: application/json

{
    "tag_name": "v1.0",
    "version_id": 3
}

Response: DatasetPointer with is_tag=true
```

**Note:** Tags are immutable - cannot be updated after creation.

### Pointer Operations (Branches + Tags)

#### 13. List All Pointers
```http
GET /api/datasets/{dataset_id}/pointers

Response: List of all branches and tags
[
    {
        "pointer_name": "main",
        "dataset_version_id": 5,
        "is_tag": false
    },
    {
        "pointer_name": "v1.0",
        "dataset_version_id": 3,
        "is_tag": true
    }
]
```

#### 14. Get Specific Pointer
```http
GET /api/datasets/{dataset_id}/pointers/{pointer_name}

Response: Pointer details
```

#### 15. Delete Pointer
```http
DELETE /api/datasets/{dataset_id}/pointers/{pointer_name}

Restrictions: Cannot delete "main" branch
Response: Success message
```

### Data Access Operations

#### 16. List Version Sheets
```http
GET /api/datasets/{dataset_id}/versions/{version_id}/sheets

Response: List of sheets/tables in the dataset
[
    {
        "name": "Sheet1",
        "row_count": 1000,
        "column_count": 10
    }
]
```

#### 17. Get Sheet Data
```http
GET /api/datasets/{dataset_id}/versions/{version_id}/data?sheet=Sheet1&limit=100&offset=0

Response: Paginated data
{
    "data": [...],
    "total_rows": 1000,
    "limit": 100,
    "offset": 0
}
```

#### 18. Get Version Schema
```http
GET /api/datasets/{dataset_id}/versions/{version_id}/schema

Response: Schema information
{
    "id": 1,
    "dataset_version_id": 456,
    "schema_json": {
        "columns": [
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "string"}
        ]
    }
}
```

### Multi-file Support

#### 19. Attach File to Version
```http
POST /api/datasets/{dataset_id}/versions/{version_id}/files
Content-Type: multipart/form-data

Parameters:
- file: File
- component_type: string (e.g., "metadata", "supplement")
- component_name: string (optional)

Response: Success with file details
```

#### 20. List Version Files
```http
GET /api/datasets/{dataset_id}/versions/{version_id}/files

Response: List of attached files
```

#### 21. Get Specific File
```http
GET /api/datasets/{dataset_id}/versions/{version_id}/files/{component_type}?component_name=optional

Response: File information
```

### Schema Operations

#### 22. Compare Schemas
```http
POST /api/datasets/{dataset_id}/schema/compare
Content-Type: application/json

{
    "version1_id": 1,
    "version2_id": 2
}

Response: Schema differences
{
    "added_columns": ["new_col"],
    "removed_columns": ["old_col"],
    "type_changes": [
        {
            "column": "amount",
            "from_type": "integer",
            "to_type": "float"
        }
    ]
}
```

## Workflow Examples

### Example 1: Basic Linear Versioning
```bash
# 1. Initial upload creates version 1 on main
POST /api/datasets/upload
{name: "Sales Data", file: sales_v1.csv}
→ Creates: version 1, main → 1

# 2. Update the dataset
POST /api/datasets/upload
{dataset_id: 123, file: sales_v2.csv, message: "Added Q4 data"}
→ Creates: version 2 (parent: 1), main → 2

# 3. View version history
GET /api/datasets/123/versions
→ Returns: [version 1, version 2]
```

### Example 2: Feature Branch Workflow
```bash
# 1. Start with main at version 2
# 2. Create feature branch
POST /api/datasets/123/branches
{branch_name: "add-customer-data", from_version_id: 2}
→ Creates: add-customer-data → 2

# 3. Commit to feature branch
POST /api/datasets/123/branches/add-customer-data/commit
{file: sales_with_customers.csv, message: "Added customer demographics"}
→ Creates: version 3 (parent: 2), add-customer-data → 3

# 4. Meanwhile, main gets updated
POST /api/datasets/123/branches/main/commit
{file: sales_v3.csv, message: "Fixed data quality issues"}
→ Creates: version 4 (parent: 2), main → 4

# Result tree:
#     1
#     ↓
#     2
#    / \
#   3   4
# (feature) (main)
```

### Example 3: Release Tagging
```bash
# 1. Tag current main as release
GET /api/datasets/123/branches/main/head
→ Returns: version 4

POST /api/datasets/123/tags
{tag_name: "v2.0-release", version_id: 4}
→ Creates: immutable tag v2.0-release → 4

# 2. Continue development on main
POST /api/datasets/123/branches/main/commit
{file: sales_v4.csv}
→ Creates: version 5, main → 5

# 3. Release version still accessible
GET /api/datasets/123/pointers/v2.0-release
→ Returns: version 4 (unchanged)
```

### Example 4: Complex Branching
```bash
# Create a tree structure:
#
# main: 1 → 2 → 5
#           ↓
#    feature: 3 → 6
#              ↓
#      hotfix: 4

# Starting from version 2 on main
POST /api/datasets/123/branches
{branch_name: "feature", from_version_id: 2}

POST /api/datasets/123/branches/feature/commit
{file: feature_data.csv}  # Creates version 3

POST /api/datasets/123/branches
{branch_name: "hotfix", from_version_id: 3}

POST /api/datasets/123/branches/hotfix/commit
{file: hotfix_data.csv}  # Creates version 4

# Continue on main
POST /api/datasets/123/branches/main/commit
{file: main_v3.csv}  # Creates version 5

# Continue on feature
POST /api/datasets/123/branches/feature/commit
{file: feature_v2.csv}  # Creates version 6
```

## Implementation Details

### File Storage Flow
1. User uploads file (any format)
2. System converts to Parquet format
3. Stores at `/data/datasets/{dataset_id}/{version_id}/{filename}.parquet`
4. Creates entry in `files` table with path
5. Links file to version via `dataset_version_files`

### Schema Capture
1. On each version creation, system reads Parquet file
2. Extracts column names, types, and metadata
3. Stores in `dataset_schema_versions` as JSON
4. Enables schema comparison between versions

### Permission Checks
- All operations check dataset permissions
- Permission types: read, write, admin
- Admin required for: delete, grant/revoke permissions
- Write required for: upload, create branches/tags

### Branch Protection
- "main" branch cannot be deleted
- Tags are immutable once created
- Deleting versions may break branch history

### Current Limitations
1. **No File Deduplication**: Each version stores complete copy
2. **No Merge Support**: Cannot merge branches
3. **No Conflict Resolution**: Last write wins
4. **Storage Growth**: Large datasets × many versions = significant storage

### Future Enhancements
- Content-addressable storage (using content_hash)
- Reference counting for deduplication
- Branch merging capabilities
- Remote storage backends (S3, Azure Blob)
- Incremental/delta storage

## Detailed Database Flow

This section explains exactly which database tables are used at each step of common operations.

### Operation 1: Initial Dataset Upload

**Endpoint:** `POST /api/datasets/upload`

**Step-by-step database operations:**

1. **Create Dataset Entry**
   ```sql
   INSERT INTO datasets (name, description, created_by, tags)
   VALUES ('Sales Data', 'Monthly sales', 1, '["finance", "sales"]')
   RETURNING id;  -- Returns dataset_id = 123
   ```

2. **Store Physical File**
   - File converted to Parquet and saved to: `/data/datasets/123/1/file.parquet`
   ```sql
   INSERT INTO files (storage_type, file_type, file_path, file_size)
   VALUES ('filesystem', 'parquet', '/data/datasets/123/1/file.parquet', 1048576)
   RETURNING id;  -- Returns file_id = 456
   ```

3. **Create Version 1**
   ```sql
   INSERT INTO dataset_versions (
       dataset_id, version_number, file_id, uploaded_by, 
       parent_version_id, message
   )
   VALUES (123, 1, 456, 1, NULL, 'Initial upload')
   RETURNING id;  -- Returns version_id = 789
   ```

4. **Link File to Version** (for multi-file support)
   ```sql
   INSERT INTO dataset_version_files (
       version_id, file_id, component_type, component_index
   )
   VALUES (789, 456, 'primary', 0);
   ```

5. **Create Default Branch**
   ```sql
   INSERT INTO dataset_pointers (
       dataset_id, pointer_name, dataset_version_id, is_tag
   )
   VALUES (123, 'main', 789, false);
   ```

6. **Capture Schema**
   ```sql
   INSERT INTO dataset_schema_versions (dataset_version_id, schema_json)
   VALUES (789, '{
       "columns": [
           {"name": "id", "type": "integer"},
           {"name": "amount", "type": "decimal"},
           {"name": "date", "type": "timestamp"}
       ]
   }');
   ```

### Operation 2: Create New Version on Main Branch

**Endpoint:** `POST /api/datasets/upload` (with dataset_id)

**Step-by-step database operations:**

1. **Get Current Branch State**
   ```sql
   SELECT dataset_version_id FROM dataset_pointers 
   WHERE dataset_id = 123 AND pointer_name = 'main';
   -- Returns: 789 (current version on main)
   ```

2. **Get Next Version Number**
   ```sql
   SELECT MAX(version_number) + 1 FROM dataset_versions 
   WHERE dataset_id = 123;
   -- Returns: 2
   ```

3. **Store New File**
   ```sql
   INSERT INTO files (storage_type, file_type, file_path, file_size)
   VALUES ('filesystem', 'parquet', '/data/datasets/123/2/file.parquet', 2097152)
   RETURNING id;  -- Returns file_id = 457
   ```

4. **Create Version 2 with Parent**
   ```sql
   INSERT INTO dataset_versions (
       dataset_id, version_number, file_id, uploaded_by,
       parent_version_id, message
   )
   VALUES (123, 2, 457, 1, 789, 'Added Q4 data')
   RETURNING id;  -- Returns version_id = 790
   ```

5. **Link File to New Version**
   ```sql
   INSERT INTO dataset_version_files (version_id, file_id, component_type)
   VALUES (790, 457, 'primary');
   ```

6. **Update Branch Pointer**
   ```sql
   UPDATE dataset_pointers 
   SET dataset_version_id = 790, updated_at = CURRENT_TIMESTAMP
   WHERE dataset_id = 123 AND pointer_name = 'main';
   ```

7. **Capture New Schema**
   ```sql
   INSERT INTO dataset_schema_versions (dataset_version_id, schema_json)
   VALUES (790, '{"columns": [...]}');
   ```

### Operation 3: Create Feature Branch

**Endpoint:** `POST /api/datasets/{id}/branches`

**Step-by-step database operations:**

1. **Verify Source Version Exists**
   ```sql
   SELECT * FROM dataset_versions 
   WHERE id = 790 AND dataset_id = 123;
   ```

2. **Create Branch Pointer**
   ```sql
   INSERT INTO dataset_pointers (
       dataset_id, pointer_name, dataset_version_id, is_tag
   )
   VALUES (123, 'feature-branch', 790, false);
   ```

**Note:** No files are copied - the branch just points to existing version 790.

### Operation 4: Commit to Feature Branch

**Endpoint:** `POST /api/datasets/{id}/branches/{branch_name}/commit`

**Step-by-step database operations:**

1. **Get Current Branch Version**
   ```sql
   SELECT dataset_version_id FROM dataset_pointers
   WHERE dataset_id = 123 AND pointer_name = 'feature-branch';
   -- Returns: 790
   ```

2. **Get Branch's Version Details** (for parent info)
   ```sql
   SELECT * FROM dataset_versions WHERE id = 790;
   ```

3. **Get Next Version Number**
   ```sql
   SELECT MAX(version_number) + 1 FROM dataset_versions
   WHERE dataset_id = 123;
   -- Returns: 3
   ```

4. **Store New File**
   ```sql
   INSERT INTO files (storage_type, file_type, file_path, file_size)
   VALUES ('filesystem', 'parquet', '/data/datasets/123/3/file.parquet', 3145728)
   RETURNING id;  -- Returns file_id = 458
   ```

5. **Create Version 3 with Parent = 790**
   ```sql
   INSERT INTO dataset_versions (
       dataset_id, version_number, file_id, uploaded_by,
       parent_version_id, message
   )
   VALUES (123, 3, 458, 1, 790, 'Added customer demographics')
   RETURNING id;  -- Returns version_id = 791
   ```

6. **Link File**
   ```sql
   INSERT INTO dataset_version_files (version_id, file_id, component_type)
   VALUES (791, 458, 'primary');
   ```

7. **Update Branch Pointer**
   ```sql
   UPDATE dataset_pointers
   SET dataset_version_id = 791, updated_at = CURRENT_TIMESTAMP
   WHERE dataset_id = 123 AND pointer_name = 'feature-branch';
   ```

8. **Capture Schema**
   ```sql
   INSERT INTO dataset_schema_versions (dataset_version_id, schema_json)
   VALUES (791, '{"columns": [...]}');
   ```

### Operation 5: Create Immutable Tag

**Endpoint:** `POST /api/datasets/{id}/tags`

**Step-by-step database operations:**

1. **Verify Version Exists**
   ```sql
   SELECT * FROM dataset_versions
   WHERE id = 791 AND dataset_id = 123;
   ```

2. **Create Tag Pointer**
   ```sql
   INSERT INTO dataset_pointers (
       dataset_id, pointer_name, dataset_version_id, is_tag
   )
   VALUES (123, 'v1.0-release', 791, true);  -- is_tag = true
   ```

**Note:** Tags cannot be updated after creation due to `is_tag = true`.

### Operation 6: Get Version Tree

**Endpoint:** `GET /api/datasets/{id}/versions/tree`

**Step-by-step database operations:**

1. **Get All Versions with Parents**
   ```sql
   SELECT id, version_number, parent_version_id, message
   FROM dataset_versions
   WHERE dataset_id = 123
   ORDER BY version_number;
   ```

2. **Get All Pointers** (to show which branches/tags point where)
   ```sql
   SELECT pointer_name, dataset_version_id, is_tag
   FROM dataset_pointers
   WHERE dataset_id = 123;
   ```

3. **Build Tree in Code**
   - Start with versions that have `parent_version_id = NULL` (roots)
   - For each version, find children where `parent_version_id = version.id`
   - Recursively build tree structure

### Operation 7: Get Branch History

**Endpoint:** `GET /api/datasets/{id}/branches/{branch_name}/history`

**Step-by-step database operations:**

1. **Get Branch HEAD**
   ```sql
   SELECT dataset_version_id FROM dataset_pointers
   WHERE dataset_id = 123 AND pointer_name = 'feature-branch';
   -- Returns: 791
   ```

2. **Traverse Parent Chain**
   ```sql
   -- First query
   SELECT * FROM dataset_versions WHERE id = 791;
   -- Returns: version 3, parent_version_id = 790
   
   -- Second query
   SELECT * FROM dataset_versions WHERE id = 790;
   -- Returns: version 2, parent_version_id = 789
   
   -- Third query
   SELECT * FROM dataset_versions WHERE id = 789;
   -- Returns: version 1, parent_version_id = NULL (root)
   ```

3. **Return History**
   - Returns: [Version 3, Version 2, Version 1]

### Operation 8: Download Version File

**Endpoint:** `GET /api/datasets/{id}/versions/{version_id}/download`

**Step-by-step database operations:**

1. **Verify Version Belongs to Dataset**
   ```sql
   SELECT * FROM dataset_versions
   WHERE id = 791 AND dataset_id = 123;
   ```

2. **Get File Information**
   ```sql
   -- Option 1: From legacy file_id
   SELECT f.* FROM files f
   JOIN dataset_versions dv ON dv.file_id = f.id
   WHERE dv.id = 791;
   
   -- Option 2: From dataset_version_files (multi-file)
   SELECT f.* FROM files f
   JOIN dataset_version_files dvf ON dvf.file_id = f.id
   WHERE dvf.version_id = 791 AND dvf.component_type = 'primary';
   ```

3. **Stream File**
   - Read from `file_path` and stream to client

### Operation 9: Compare Schemas

**Endpoint:** `POST /api/datasets/{id}/schema/compare`

**Step-by-step database operations:**

1. **Get Schema for Version 1**
   ```sql
   SELECT schema_json FROM dataset_schema_versions
   WHERE dataset_version_id = 789;
   ```

2. **Get Schema for Version 2**
   ```sql
   SELECT schema_json FROM dataset_schema_versions
   WHERE dataset_version_id = 790;
   ```

3. **Compare in Code**
   - Extract columns from both schemas
   - Find added columns (in v2 but not v1)
   - Find removed columns (in v1 but not v2)
   - Find type changes (same column, different type)

### Operation 10: Delete Version

**Endpoint:** `DELETE /api/datasets/{id}/versions/{version_id}`

**Step-by-step database operations:**

1. **Check No Branches Point to This Version**
   ```sql
   SELECT COUNT(*) FROM dataset_pointers
   WHERE dataset_version_id = 791;
   ```

2. **Check No Children Depend on This Version**
   ```sql
   SELECT COUNT(*) FROM dataset_versions
   WHERE parent_version_id = 791;
   ```

3. **Delete Schema**
   ```sql
   DELETE FROM dataset_schema_versions
   WHERE dataset_version_id = 791;
   ```

4. **Delete File Links**
   ```sql
   DELETE FROM dataset_version_files
   WHERE version_id = 791;
   ```

5. **Delete Version**
   ```sql
   DELETE FROM dataset_versions
   WHERE id = 791;
   ```

6. **Delete Physical Files**
   - Remove files from `/data/datasets/123/3/`
   
7. **Update File Reference Count** (if implemented)
   ```sql
   UPDATE files SET reference_count = reference_count - 1
   WHERE id IN (SELECT file_id FROM dataset_version_files WHERE version_id = 791);
   ```

### Table Usage Summary

| Table | Primary Use | Key Relationships |
|-------|-------------|-------------------|
| `datasets` | Base dataset metadata | Parent of all versions |
| `dataset_versions` | Each version/commit | Has parent_version_id for DAG |
| `dataset_pointers` | Branches and tags | Points to specific versions |
| `files` | Physical file storage | Referenced by versions |
| `dataset_version_files` | Multi-file support | Links versions to multiple files |
| `dataset_schema_versions` | Schema tracking | One per version |

## Summary

This versioning system provides Git-like semantics for dataset management, enabling:
- Parallel development through branches
- Immutable snapshots through tags
- Full version history and lineage
- Schema evolution tracking
- Multi-file dataset support

While some endpoints may seem redundant, they provide different access patterns and use cases for enterprise data management scenarios.