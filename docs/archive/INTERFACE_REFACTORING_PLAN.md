# Interface Refactoring Plan

## Overview
This plan addresses the interface design issues identified in the codebase analysis, focusing on fixing direct dependencies, creating missing interfaces, and improving existing ones.

## Phase 1: Critical Fixes (High Priority) ✅ COMPLETED

### 1.1 Fix Direct Concrete Dependencies
- [x] **authorization.py** - Replace `PostgresUserRepository` with `IUserRepository`
- [x] **create_user_public.py** - Use dependency injection for `IUserRepository`
- [x] **download_dataset.py** - Replace `PostgresTableReader` with `ITableReader`

### 1.2 Create Missing Critical Interfaces
- [x] Create `IPasswordManager` interface in `/core/abstractions/external.py`
- [x] Implement `PasswordHasher` in `/infrastructure/external/password_hasher.py`
- [x] Update all password-related code to use the interface
  - Updated `create_user_public.py`
  - Updated `login_user.py`
  - Updated `create_user.py`
  - Updated `update_user.py`

## Phase 2: Interface Segregation (Medium Priority) ✅ COMPLETED

### 2.1 Split Large Interfaces
- [x] Create new focused interfaces:
  - `ITableMetadataReader` - metadata operations only
  - `ITableDataReader` - data reading operations
  - `ITableAnalytics` - statistics and analysis
  - ~~`ITableWriter`~~ - write operations (not needed)
- [x] Update `PostgresTableReader` to implement all new interfaces
- [x] Update all consumers to use appropriate interfaces
  - Updated `get_dataset_overview.py` to use `ITableMetadataReader`
  - Updated `download_dataset.py` to use both `ITableMetadataReader` and `ITableDataReader`
  - Updated `download_table.py` to use both `ITableMetadataReader` and `ITableDataReader`

### 2.2 Split ICommitRepository
- [x] Create separate interfaces:
  - `ICommitOperations` - commit operations only
  - `IRefOperations` - ref/branch operations
  - `IManifestOperations` - manifest operations
- [x] Update implementations and consumers
  - Updated `PostgresCommitRepository` to implement all three interfaces
  - Added missing `create_commit` method to satisfy `ICommitOperations`

## Phase 3: Remove Leaky Abstractions ✅ COMPLETED

### 3.1 Remove Infrastructure-Specific Types
- [x] Replace `asyncpg.Connection` with generic connection interface
  - Created `IDatabaseConnection`, `ITransaction`, and `IDatabasePool` interfaces
  - Created adapters to wrap asyncpg types
  - Updated `DatabasePool` to implement `IDatabasePool`
  - Updated `BasePostgresRepository` to work with both interfaces
- [x] Replace `asyncpg.Record` with `Dict[str, Any]`
  - All interface methods now return `Dict[str, Any]` instead of `asyncpg.Record`
  - Adapters handle conversion automatically
- [ ] Create abstraction for JSONB operations

### 3.2 Create Worker Interfaces
- [x] Create `IImportExecutor` interface
- [x] Create `IJobExecutor` base interface
- [x] Update workers to implement interfaces
  - Note: Worker interfaces were included in the JSONB abstraction file for completeness

## Phase 4: Infrastructure Improvements

### 4.1 Dependency Injection Enhancement
- [ ] Create factory pattern for repository creation
- [ ] Standardize DI approach across all handlers
- [ ] Consider implementing a DI container

### 4.2 Event System Refactoring
- [ ] Create `IEventBus` interface
- [ ] Remove global singleton pattern
- [ ] Inject event bus through DI

## Implementation Order

### Week 1: Critical Fixes
1. Fix direct dependencies (3 files)
2. Create and implement `IPasswordManager`
3. Test affected endpoints

### Week 2: Interface Segregation
1. Split `ITableReader` interface
2. Update implementations
3. Refactor consumers to use new interfaces

### Week 3: Infrastructure Updates
1. Remove asyncpg types from interfaces
2. Create worker interfaces
3. Implement event bus interface

### Week 4: Testing & Documentation
1. Add interface contract tests
2. Update documentation
3. Create integration tests

## Success Metrics
- Zero direct concrete dependencies in feature layer
- All interfaces follow Single Responsibility Principle
- No infrastructure types in core abstractions
- 100% interface test coverage
- Improved modularity and testability

## Risk Mitigation
- Create feature flags for gradual rollout
- Maintain backward compatibility during transition
- Run parallel tests with old and new implementations
- Document all interface changes

## Detailed Interface Designs

### IPasswordManager Interface
```python
from abc import ABC, abstractmethod

class IPasswordManager(ABC):
    """Interface for password hashing and verification."""
    
    @abstractmethod
    def hash_password(self, password: str) -> str:
        """Hash a plain text password."""
        pass
    
    @abstractmethod
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a plain text password against a hashed password."""
        pass
```

### Split Table Reader Interfaces
```python
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

class ITableMetadataReader(ABC):
    """Read-only interface for table metadata operations."""
    
    @abstractmethod
    async def list_table_keys(self, commit_id: str) -> List[str]:
        """List all table keys for a commit."""
        pass
    
    @abstractmethod
    async def get_table_schema(self, commit_id: str, table_key: str) -> Optional[Dict[str, Any]]:
        """Get schema for a specific table."""
        pass
    
    @abstractmethod
    async def get_table_row_count(self, commit_id: str, table_key: str) -> int:
        """Get row count for a table."""
        pass

class ITableDataReader(ABC):
    """Interface for reading table data."""
    
    @abstractmethod
    async def get_table_data(
        self, 
        commit_id: str, 
        table_key: str, 
        offset: int = 0, 
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Read table data with pagination."""
        pass
    
    @abstractmethod
    async def get_table_sample(
        self, 
        commit_id: str, 
        table_key: str, 
        sample_size: int
    ) -> List[Dict[str, Any]]:
        """Get a random sample of table data."""
        pass

class ITableAnalytics(ABC):
    """Interface for table analytics operations."""
    
    @abstractmethod
    async def get_table_statistics(
        self, 
        commit_id: str, 
        table_key: str
    ) -> Optional[Dict[str, Any]]:
        """Get statistical analysis of table data."""
        pass
    
    @abstractmethod
    async def get_column_statistics(
        self, 
        commit_id: str, 
        table_key: str, 
        column_name: str
    ) -> Optional[Dict[str, Any]]:
        """Get statistics for a specific column."""
        pass
```

### Worker Interfaces
```python
from abc import ABC, abstractmethod
from typing import Dict, Any
from uuid import UUID

class IJobExecutor(ABC):
    """Base interface for all job executors."""
    
    @abstractmethod
    async def execute(self, job_id: UUID, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a job with given parameters."""
        pass
    
    @abstractmethod
    def get_job_type(self) -> str:
        """Return the job type this executor handles."""
        pass

class IImportExecutor(IJobExecutor):
    """Interface for import job execution."""
    
    @abstractmethod
    async def validate_file(self, file_path: str) -> Dict[str, Any]:
        """Validate file before import."""
        pass
    
    @abstractmethod
    async def parse_file(self, file_path: str) -> Dict[str, Any]:
        """Parse file and return metadata."""
        pass
```

## Notes
- This plan ensures systematic improvement of the interface design while maintaining system stability
- Each phase builds upon the previous one
- Backward compatibility is maintained throughout the refactoring process
- All changes should be accompanied by appropriate tests