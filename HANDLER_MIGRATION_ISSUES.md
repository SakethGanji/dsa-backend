# Handler Migration Issues - API Layer Refactoring

## Overview
This document identifies all API endpoints that implement business logic directly instead of delegating to handlers. Each problem area is marked with specific line numbers and examples.

## 🔴 Critical Issues (Files with Heavy Business Logic)

### 1. `api/datasets.py`
**Status**: ❌ Mostly implements business logic directly despite having handlers available

#### Problem Areas:

##### `POST /datasets` (lines 78-172)
- **Issue**: Implements all business logic directly in endpoint
- **Current problems**:
  - Direct repository calls: `dataset_repo.get_dataset_by_name_and_user()` (line 116)
  - Transaction management in endpoint (line 134)
  - Permission granting logic (lines 156-170)
  - Search index refresh in endpoint (line 151)
- **Available handler**: `CreateDatasetHandler` exists but unused
- **Fix**: Delegate to existing handler

##### `PUT /datasets/{dataset_id}` (lines 175-224)
- **Issue**: Direct repository updates
- **Current problems**:
  - Permission checking in endpoint (line 196)
  - Direct repository call: `dataset_repo.update_dataset()` (line 213)
- **Available handler**: `UpdateDatasetHandler` exists but unused

##### `DELETE /datasets/{dataset_id}` (lines 227-262)
- **Issue**: Complex deletion logic in endpoint
- **Current problems**:
  - Permission checking (line 247)
  - Direct repository calls for deletion
- **Available handler**: Check if `DeleteDatasetHandler` exists

##### `POST /datasets/{dataset_id}/permissions` (lines 265-298)
- **Issue**: Uses handler but has validation logic in endpoint
- **Current**: Partially uses `GrantPermissionHandler`
- **Fix**: Move all validation to handler

##### `POST /datasets/upload` (lines 301-390)
- **Issue**: Heavy file processing logic
- **Current problems**:
  - File validation in endpoint
  - CSV/Excel parsing logic
  - Direct repository calls
- **Fix**: Create `UploadDatasetHandler`

### 2. `api/downloads.py`
**Status**: ❌ No handlers used at all

#### Problem Areas:

##### `GET /downloads/{dataset_id}/csv` (lines 45-97)
- **Issue**: Complex CSV generation logic
- **Current problems**:
  - Data fetching and transformation
  - CSV generation in endpoint
- **Fix**: Create `ExportCsvHandler`

##### `GET /downloads/{dataset_id}/excel` (lines 100-159)
- **Issue**: Excel generation logic
- **Current problems**:
  - Complex Excel formatting
  - Data transformation
- **Fix**: Create `ExportExcelHandler`

##### `GET /downloads/{dataset_id}/json` (lines 162-211)
- **Issue**: JSON export logic
- **Current problems**:
  - Data fetching and formatting
  - Streaming response logic
- **Fix**: Create `ExportJsonHandler`

### 3. `api/exploration.py`
**Status**: ❌ Raw SQL queries in endpoints

#### Problem Areas:

##### `POST /exploration/preview` (lines 38-102)
- **Issue**: SQL query building and execution
- **Current problems**:
  - Direct database access
  - Query validation in endpoint
- **Fix**: Create `PreviewDataHandler`

##### `POST /exploration/aggregate` (lines 105-189)
- **Issue**: Complex aggregation logic
- **Current problems**:
  - SQL query construction
  - Result transformation
- **Fix**: Create `AggregateDataHandler`

##### `POST /exploration/filter` (lines 192-245)
- **Issue**: Filter query building
- **Fix**: Create `FilterDataHandler`

### 4. `api/search.py`
**Status**: ⚠️ Creates handlers inline instead of using dedicated handler classes

#### Problem Areas:

##### `GET /search` (lines 47-93)
- **Issue**: Search logic and validation in endpoint
- **Current problems**:
  - Query parsing in endpoint
  - Result filtering logic
- **Fix**: Create proper `SearchDatasetsHandler`

##### `POST /search/advanced` (lines 96-152)
- **Issue**: Complex search query building
- **Fix**: Create `AdvancedSearchHandler`

## 🟡 Partial Issues (Mixed Implementation)

### 5. `api/sampling.py`
**Status**: ⚠️ Uses handlers for some operations but not all

#### Problem Areas:

##### `POST /sampling/{dataset_id}/start` (lines 45-89)
- **Issue**: Direct service calls
- **Current**: Calls `sampling_service` directly
- **Fix**: Create `StartSamplingHandler`

##### `GET /sampling/{dataset_id}/status` (lines 92-125)
- **Issue**: Status checking logic in endpoint
- **Fix**: Create `GetSamplingStatusHandler`

### 6. `api/users.py`
**Status**: ⚠️ Mostly uses handlers but has one problematic endpoint

#### Problem Areas:

##### `POST /auth/register` (lines 156-195)
- **Issue**: Direct repository calls in public registration
- **Current**: `create_user_public` implements logic directly
- **Fix**: Use existing `CreateUserHandler`

## 🟢 Good Examples (Properly Using Handlers)

### `api/versioning.py`
- ✅ All endpoints delegate to handlers
- ✅ Clean separation of concerns
- ✅ Example pattern to follow

### `api/jobs.py`
- ✅ Properly uses `GetJobsHandler` and `GetJobByIdHandler`
- ✅ Minimal endpoint logic

### `api/workbench.py`
- ✅ Uses `PreviewSqlHandler` and `TransformSqlHandler`
- ✅ Good example of SQL-related handlers

## Migration Priority

1. **High Priority** (Implement immediately):
   - `api/datasets.py` - Has handlers but doesn't use them
   - `api/downloads.py` - Complex logic needs handlers
   - `api/exploration.py` - SQL queries need proper abstraction

2. **Medium Priority**:
   - `api/search.py` - Improper handler usage
   - `api/sampling.py` - Partial handler usage

3. **Low Priority**:
   - `api/users.py` - Only one endpoint needs fixing

## Next Steps

1. Start with `datasets.py` since handlers already exist
2. Create missing handlers for `downloads.py` and `exploration.py`
3. Ensure all endpoints follow the pattern from `versioning.py`
4. Add architecture tests to prevent regression