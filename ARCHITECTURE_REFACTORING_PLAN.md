# Final Architecture Refactoring Plan

## Executive Summary

This document consolidates all architectural improvements into a single, actionable plan that addresses:
- Handler inconsistencies and business logic placement
- Removal of misused CQRS patterns
- Service layer standardization
- Dependency injection for API and worker consistency

## Context: Why This Refactoring Is Needed

### Current Problems

#### 1. Misused CQRS Pattern
The codebase attempts to use CQRS (Command Query Responsibility Segregation) but implements it incorrectly:
- **Commands that are actually queries**: `GetDatasetCommand`, `ListDatasetsCommand`
- **No real separation**: Same database, same models, no event sourcing
- **Added complexity without benefits**: Extra abstraction layers that just mirror API requests
- **Confusing terminology**: Developers waste time understanding unnecessary patterns

#### 2. Handler Inconsistencies
Analysis of handlers across features reveals:
- **No consistent base class usage**: Some use `BaseHandler`, others have no inheritance
- **Business logic in wrong places**: Complex logic in handlers instead of services
- **Transaction management chaos**: Mix of decorators, manual management, and no management
- **Permission checking inconsistency**: Some handlers check internally, others assume external
- **Event publishing gaps**: State changes without audit trails

#### 3. Specific Examples of Problems

**Downloads Feature** - No base class, manual error handling:
```python
# Current problematic code
class DownloadDatasetHandler:
    async def handle(self, request: DownloadDatasetRequest):
        async with self._uow as uow:  # Manual transaction
            dataset = await uow.datasets.get_by_id(request.dataset_id)
            if not dataset:
                raise NotFoundError("Dataset not found")  # Inconsistent error
```

**Sampling Feature** - Dangerous async/sync mixing:
```python
# Current problematic code  
async def handle(self, command: Command):
    data = self._generate_sample_sync(command.config)  # Blocks event loop!
    result = await self._save_async(data)
```

**Jobs Feature** - Mixed patterns within same feature:
```python
# Some handlers use decorators
@with_transaction
async def handle(self, command): ...

# Others don't
async def handle(self, command): ...  # No transaction management!
```

## Target Architecture

### Key Principles

1. **Services contain ALL business logic** - No logic in handlers, API routes, or workers
2. **Direct service calls from API** - No handler layer needed for 90%+ of cases
3. **Centralized dependency injection** - Single source of truth for service construction
4. **Feature-based organization** - All related code co-located within features
5. **No CQRS abstractions** - Simple, direct method calls instead of commands/queries

### Target Directory Structure

```
./src
├── api/
│   ├── dependencies.py      # Uses container.py to provide services to FastAPI
│   ├── error_handlers.py
│   └── routers/            # One router per feature
│       ├── datasets.py     # Directly calls DatasetService
│       ├── users.py        # Directly calls UserService
│       ├── jobs.py
│       └── ...
├── container.py            # NEW: Central dependency injection factories
├── core/
│   ├── auth/              # Grouped auth logic
│   │   ├── password.py
│   │   └── security.py
│   ├── decorators.py      # @with_transaction, @with_error_handling
│   ├── domain_exceptions.py
│   ├── events/
│   └── permissions.py
├── features/
│   ├── base_service.py    # NEW: Base class for all services
│   ├── datasets/
│   │   ├── models.py      # Request/Response DTOs (Pydantic)
│   │   └── services.py    # ALL dataset business logic
│   ├── users/
│   │   ├── models.py
│   │   └── services.py
│   ├── jobs/
│   │   ├── models.py
│   │   └── services.py
│   └── ... (other features follow same pattern)
├── infrastructure/
│   ├── config/
│   │   └── settings.py
│   ├── postgres/
│   │   ├── repositories/  # All repository implementations
│   │   │   ├── dataset_repo.py
│   │   │   └── user_repo.py
│   │   ├── uow.py        # Enhanced with event collection
│   │   └── database.py
│   └── external/         # Third-party integrations
├── main.py
└── workers/
    ├── dataset_tasks.py  # Thin orchestrators using services
    ├── job_tasks.py
    └── ...
```

### What Gets Removed

- ❌ `/src/services/` directory (logic moves to feature services)
- ❌ `/src/features/*/handlers/` directories
- ❌ `/src/features/*/models/commands.py` files
- ❌ Command/Query objects and CQRS patterns
- ❌ `BaseHandler`, `BaseUpdateHandler` classes

## Implementation Phases

### Phase 0: Foundation Setup (Week 1)

#### 0.1 Create Core Infrastructure

**Create `src/container.py`:**
```python
# src/container.py
from functools import lru_cache
from src.infrastructure.config.settings import Settings
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.core.permissions import PermissionService
from src.core.events.publisher import EventBus

@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

def get_permission_service(uow: PostgresUnitOfWork) -> PermissionService:
    return PermissionService(uow=uow)

def get_dataset_service(uow: PostgresUnitOfWork, event_bus: EventBus = None):
    # Local import prevents circular dependency issues
    from src.features.datasets.services import DatasetService
    permissions = get_permission_service(uow)
    return DatasetService(uow=uow, permissions=permissions, event_bus=event_bus)

def get_user_service(uow: PostgresUnitOfWork, event_bus: EventBus = None):
    # Local import prevents circular dependency issues
    from src.features.users.services import UserService
    permissions = get_permission_service(uow)
    return UserService(uow=uow, permissions=permissions, event_bus=event_bus)

# Add factories for all other services...
# Note: Local imports inside factory functions prevent circular dependencies
```

**Move decorators to `src/core/decorators.py`:**
```python
# src/core/decorators.py
from functools import wraps
import logging

logger = logging.getLogger(__name__)

def with_transaction(func):
    """Decorator for automatic transaction management."""
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        if hasattr(self, '_uow'):
            await self._uow.begin()
            try:
                result = await func(self, *args, **kwargs)
                await self._uow.commit()
                # Publish any collected events after successful commit
                await self._uow.publish_collected_events()
                return result
            except Exception:
                await self._uow.rollback()
                raise
        return await func(self, *args, **kwargs)
    return wrapper

def with_error_handling(func):
    """Decorator for consistent error handling."""
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        try:
            return await func(self, *args, **kwargs)
        except DomainException:
            raise  # Re-raise domain exceptions as-is
        except ValueError as e:
            raise ValidationException(str(e))
        except PermissionError as e:
            raise ForbiddenException(str(e))
        except Exception as e:
            logger.error(f"Unexpected error in {self.__class__.__name__}.{func.__name__}", exc_info=True)
            raise DomainException("An unexpected error occurred")
    return wrapper
```

#### 0.2 Enhance UnitOfWork for Event Collection

**Update `src/infrastructure/postgres/uow.py`:**
```python
class PostgresUnitOfWork:
    def __init__(self, session, event_bus=None):
        self._session = session
        self._event_bus = event_bus
        self._collected_events = []
        # Initialize repositories...
    
    def collect_event(self, event: DomainEvent):
        """Collect events to be published after transaction commits."""
        self._collected_events.append(event)
    
    async def publish_collected_events(self):
        """Publish all collected events (called after commit)."""
        if self._event_bus:
            for event in self._collected_events:
                await self._event_bus.publish(event)
        self._collected_events.clear()
    
    async def rollback(self):
        await self._session.rollback()
        self._collected_events.clear()  # Don't publish events on rollback
```

#### 0.3 Update API Dependencies

**Update `src/api/dependencies.py`:**
```python
from typing import AsyncGenerator
from fastapi import Depends
from src.infrastructure.postgres.database import get_session
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.container import get_dataset_service, get_user_service, get_job_service

async def get_uow() -> AsyncGenerator[PostgresUnitOfWork, None]:
    async with get_session() as session:
        yield PostgresUnitOfWork(session)

def get_dataset_service_api(
    uow: PostgresUnitOfWork = Depends(get_uow),
    event_bus: EventBus = Depends(get_event_bus)
) -> DatasetService:
    return get_dataset_service(uow, event_bus)

def get_user_service_api(
    uow: PostgresUnitOfWork = Depends(get_uow),
    event_bus: EventBus = Depends(get_event_bus)
) -> UserService:
    return get_user_service(uow, event_bus)

# Add for all other services...
```

#### 0.4 Create Base Service Class

**Create `src/features/base_service.py`:**
```python
# src/features/base_service.py
from typing import Optional
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.core.permissions import PermissionService
from src.core.events.publisher import EventBus

class BaseService:
    """Base class for all services providing standard dependencies."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        permissions: PermissionService,
        event_bus: Optional[EventBus] = None
    ):
        self._uow = uow
        self._permissions = permissions
        self._event_bus = event_bus
```

### Phase 1: Pilot Feature Refactoring - Users (Week 2)

Use the Users feature as a template for all others.

#### 1.1 Create User Service

**Create `src/features/users/services.py`:**
```python
from typing import Optional
from datetime import datetime
from src.features.base_service import BaseService
from src.core.decorators import with_transaction, with_error_handling
from .models import CreateUserRequest, UpdateUserRequest, UserResponse, LoginRequest, LoginResponse

class UserService(BaseService):
    """Service containing all user-related business logic."""
    # No need to define __init__ - inherited from BaseService!
    
    @with_error_handling
    @with_transaction
    async def create_user(
        self,
        name: str,
        email: str,
        password: str,
        role: str = "user"
    ) -> UserResponse:
        """Create a new user."""
        # Check if email already exists
        existing = await self._uow.users.get_by_email(email)
        if existing:
            raise BusinessRuleViolation(f"User with email {email} already exists")
        
        # Create user entity
        user = User(
            id=generate_uuid(),
            name=name,
            email=email,
            password_hash=hash_password(password),
            role=role,
            created_at=datetime.utcnow()
        )
        
        # Save to repository
        await self._uow.users.add(user)
        
        # Collect event for publishing after commit
        self._uow.collect_event(UserCreatedEvent(
            user_id=user.id,
            email=user.email,
            timestamp=datetime.utcnow()
        ))
        
        return UserResponse(
            id=user.id,
            name=user.name,
            email=user.email,
            role=user.role,
            created_at=user.created_at
        )
    
    @with_error_handling
    async def login(self, email: str, password: str) -> LoginResponse:
        """Authenticate user and generate tokens."""
        # No transaction needed for read operation
        user = await self._uow.users.get_by_email(email)
        if not user or not verify_password(password, user.password_hash):
            raise ValidationException("Invalid credentials")
        
        # Generate tokens
        access_token = create_access_token(user.id, user.role)
        refresh_token = create_refresh_token(user.id)
        
        return LoginResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            user_id=user.id,
            email=user.email,
            role=user.role
        )
    
    @with_error_handling
    @with_transaction
    async def update_user(
        self,
        user_id: str,
        requester_id: str,
        name: Optional[str] = None,
        email: Optional[str] = None
    ) -> UserResponse:
        """Update user information."""
        # Check permissions
        if user_id != requester_id:
            await self._permissions.require("user", user_id, requester_id, "admin")
        
        # Get and update user
        user = await self._uow.users.get_by_id(user_id)
        if not user:
            raise EntityNotFoundException("User", user_id)
        
        if name:
            user.name = name
        if email and email != user.email:
            # Check email uniqueness
            existing = await self._uow.users.get_by_email(email)
            if existing:
                raise BusinessRuleViolation(f"Email {email} already in use")
            user.email = email
        
        user.updated_at = datetime.utcnow()
        await self._uow.users.update(user)
        
        # Collect event
        self._uow.collect_event(UserUpdatedEvent(
            user_id=user.id,
            updated_by=requester_id,
            timestamp=datetime.utcnow()
        ))
        
        return UserResponse.from_entity(user)
    
    # Add other user operations...
```

#### 1.2 Update Models

**Update `src/features/users/models.py`:**
```python
from pydantic import BaseModel, EmailStr, Field
from datetime import datetime
from typing import Optional

# Request Models
class CreateUserRequest(BaseModel):
    name: str = Field(min_length=2, max_length=100)
    email: EmailStr
    password: str = Field(min_length=8)
    role: str = "user"

class UpdateUserRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=2, max_length=100)
    email: Optional[EmailStr] = None

class LoginRequest(BaseModel):
    email: EmailStr
    password: str

# Response Models
class UserResponse(BaseModel):
    id: str
    name: str
    email: str
    role: str
    created_at: datetime
    updated_at: Optional[datetime] = None
    
    @classmethod
    def from_entity(cls, user):
        return cls(
            id=user.id,
            name=user.name,
            email=user.email,
            role=user.role,
            created_at=user.created_at,
            updated_at=user.updated_at
        )

class LoginResponse(BaseModel):
    access_token: str
    refresh_token: str
    user_id: str
    email: str
    role: str
```

#### 1.3 Update API Router

**Update `src/api/routers/users.py`:**
```python
from fastapi import APIRouter, Depends, status
from src.features.users.models import (
    CreateUserRequest, UpdateUserRequest, UserResponse, 
    LoginRequest, LoginResponse
)
from src.features.users.services import UserService
from src.api.dependencies import get_user_service_api, get_current_user

router = APIRouter(prefix="/users", tags=["users"])

@router.post("/", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def create_user(
    request: CreateUserRequest,
    service: UserService = Depends(get_user_service_api)
) -> UserResponse:
    """Create a new user."""
    return await service.create_user(
        name=request.name,
        email=request.email,
        password=request.password,
        role=request.role
    )

@router.post("/login", response_model=LoginResponse)
async def login(
    request: LoginRequest,
    service: UserService = Depends(get_user_service_api)
) -> LoginResponse:
    """Authenticate user and get tokens."""
    return await service.login(
        email=request.email,
        password=request.password
    )

@router.put("/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: str,
    request: UpdateUserRequest,
    current_user = Depends(get_current_user),
    service: UserService = Depends(get_user_service_api)
) -> UserResponse:
    """Update user information."""
    return await service.update_user(
        user_id=user_id,
        requester_id=current_user.id,
        name=request.name,
        email=request.email
    )

# No handlers! Direct service calls only
```

#### 1.4 Clean Up

1. Delete `src/features/users/handlers/` directory
2. Delete `src/features/users/models/commands.py`
3. Update tests to test services directly

### Phase 2: Refactor Remaining Features (Weeks 3-5)

Apply the same pattern to each feature. **CRITICAL**: Test every endpoint after each handler migration to ensure nothing breaks.

#### 2.1 Datasets Feature

**Step 1: Create DatasetService**
```python
# src/features/datasets/services.py
from src.features.base_service import BaseService
from src.core.decorators import with_transaction, with_error_handling

class DatasetService(BaseService):
    @with_error_handling
    @with_transaction
    async def create_dataset(self, name: str, description: str, tags: List[str], user_id: str) -> DatasetResponse:
        # Move logic from CreateDatasetHandler
        pass
    
    @with_error_handling
    async def get_dataset(self, dataset_id: str, user_id: str) -> DatasetResponse:
        # Move logic from GetDatasetHandler
        pass
    
    @with_error_handling
    async def list_datasets(self, user_id: str, offset: int = 0, limit: int = 100) -> Tuple[List[DatasetResponse], int]:
        # Move logic from ListDatasetsHandler
        pass
    
    @with_error_handling
    @with_transaction
    async def update_dataset(self, dataset_id: str, user_id: str, **updates) -> DatasetResponse:
        # Move logic from UpdateDatasetHandler
        pass
    
    @with_error_handling
    @with_transaction
    async def delete_dataset(self, dataset_id: str, user_id: str) -> bool:
        # Move logic from DeleteDatasetHandler
        pass
    
    @with_error_handling
    @with_transaction
    async def grant_permission(self, dataset_id: str, grantor_id: str, user_id: str, permission: str) -> bool:
        # Move logic from GrantPermissionHandler
        pass
```

**Step 2: Update API Router**
```python
# src/api/routers/datasets.py
@router.post("/", response_model=DatasetResponse)
async def create_dataset(
    request: CreateDatasetRequest,
    current_user = Depends(get_current_user),
    service: DatasetService = Depends(get_dataset_service_api)
):
    return await service.create_dataset(
        name=request.name,
        description=request.description,
        tags=request.tags,
        user_id=current_user.id
    )
# Update all other endpoints similarly...
```

**Step 3: Test Each Endpoint**
```bash
# Test dataset creation
curl -X POST "http://localhost:8000/api/v1/datasets" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "Test Dataset", "description": "Test", "tags": ["test"]}'

# Test dataset retrieval
curl -X GET "http://localhost:8000/api/v1/datasets/{dataset_id}" \
  -H "Authorization: Bearer $TOKEN"

# Test dataset listing
curl -X GET "http://localhost:8000/api/v1/datasets?offset=0&limit=10" \
  -H "Authorization: Bearer $TOKEN"

# Test dataset update
curl -X PUT "http://localhost:8000/api/v1/datasets/{dataset_id}" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "Updated Name", "description": "Updated"}'

# Test dataset deletion
curl -X DELETE "http://localhost:8000/api/v1/datasets/{dataset_id}" \
  -H "Authorization: Bearer $TOKEN"

# Test permission grant
curl -X POST "http://localhost:8000/api/v1/datasets/{dataset_id}/permissions" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user123", "permission": "read"}'
```

**Step 4: Clean Up**
- Delete `src/features/datasets/handlers/` directory
- Delete `src/features/datasets/models/commands.py`
- Update `src/features/datasets/models.py` to only contain request/response models

#### 2.2 Jobs Feature

**Step 1: Create JobService**
```python
# src/features/jobs/services.py
from src.features.base_service import BaseService
from src.core.decorators import with_transaction, with_error_handling

class JobService(BaseService):
    @with_error_handling
    @with_transaction
    async def create_job(self, job_type: str, dataset_id: str, user_id: str, parameters: dict) -> JobResponse:
        # Move logic from CreateJobHandler
        # Validate job type, check permissions, create job, publish event
        pass
    
    @with_error_handling
    async def get_job(self, job_id: str, user_id: str) -> JobResponse:
        # Move logic from GetJobHandler
        pass
    
    @with_error_handling
    async def list_jobs(self, user_id: str, dataset_id: Optional[str] = None, status: Optional[str] = None) -> List[JobResponse]:
        # Move logic from ListJobsHandler
        pass
    
    @with_error_handling
    @with_transaction
    async def cancel_job(self, job_id: str, user_id: str) -> JobResponse:
        # Move logic from CancelJobHandler
        pass
    
    @with_error_handling
    async def get_job_status(self, job_id: str, user_id: str) -> JobStatusResponse:
        # Move logic from GetJobStatusHandler
        pass
```

**Step 2: Test Each Endpoint**
```bash
# Test job creation
curl -X POST "http://localhost:8000/api/v1/jobs" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"type": "import", "dataset_id": "123", "parameters": {"file_path": "/tmp/data.csv"}}'

# Test job retrieval
curl -X GET "http://localhost:8000/api/v1/jobs/{job_id}" \
  -H "Authorization: Bearer $TOKEN"

# Test job listing
curl -X GET "http://localhost:8000/api/v1/jobs?dataset_id=123&status=running" \
  -H "Authorization: Bearer $TOKEN"

# Test job cancellation
curl -X POST "http://localhost:8000/api/v1/jobs/{job_id}/cancel" \
  -H "Authorization: Bearer $TOKEN"

# Test job status
curl -X GET "http://localhost:8000/api/v1/jobs/{job_id}/status" \
  -H "Authorization: Bearer $TOKEN"
```

#### 2.3 Versioning Feature

**Step 1: Create VersioningService**
```python
# src/features/versioning/services.py
class VersioningService(BaseService):
    @with_error_handling
    @with_transaction
    async def create_commit(self, dataset_id: str, user_id: str, message: str, changes: dict) -> CommitResponse:
        # Move logic from CreateCommitHandler
        # Complex logic for manifest creation, hashing, etc.
        pass
    
    @with_error_handling
    async def get_commit_history(self, dataset_id: str, user_id: str, ref_name: str = "main") -> List[CommitResponse]:
        # Move logic from GetCommitHistoryHandler
        pass
    
    @with_error_handling
    @with_transaction
    async def checkout_commit(self, dataset_id: str, user_id: str, commit_id: str, branch_name: str) -> CheckoutResponse:
        # Move logic from CheckoutCommitHandler
        pass
    
    @with_error_handling
    async def get_dataset_overview(self, dataset_id: str, user_id: str) -> DatasetOverviewResponse:
        # Move logic from GetDatasetOverviewHandler
        pass
    
    @with_error_handling
    async def get_table_data(self, dataset_id: str, table_key: str, commit_id: str, user_id: str, offset: int = 0, limit: int = 100) -> TableDataResponse:
        # Move logic from GetTableDataHandler
        pass
```

**Step 2: Test Each Endpoint**
```bash
# Test commit creation
curl -X POST "http://localhost:8000/api/v1/datasets/{dataset_id}/commits" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"message": "Added new data", "changes": {"data": [["row1"], ["row2"]]}}'

# Test commit history
curl -X GET "http://localhost:8000/api/v1/datasets/{dataset_id}/commits?ref=main" \
  -H "Authorization: Bearer $TOKEN"

# Test checkout
curl -X POST "http://localhost:8000/api/v1/datasets/{dataset_id}/checkout" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"commit_id": "abc123", "branch_name": "feature-branch"}'

# Test dataset overview
curl -X GET "http://localhost:8000/api/v1/datasets/{dataset_id}/overview" \
  -H "Authorization: Bearer $TOKEN"

# Test table data
curl -X GET "http://localhost:8000/api/v1/datasets/{dataset_id}/tables/{table_key}/data?commit_id=abc123&offset=0&limit=50" \
  -H "Authorization: Bearer $TOKEN"
```

#### 2.4 Downloads Feature

**Step 1: Create DownloadService**
```python
# src/features/downloads/services.py
class DownloadService(BaseService):
    @with_error_handling
    async def download_dataset(self, dataset_id: str, user_id: str, format: str = "csv") -> DownloadResponse:
        # Move logic from DownloadDatasetHandler
        # Check permissions, get data, format, return file
        pass
    
    @with_error_handling
    async def download_table(self, dataset_id: str, table_key: str, user_id: str, format: str = "csv") -> DownloadResponse:
        # Move logic from DownloadTableHandler
        pass
    
    @with_error_handling
    async def export_data(self, dataset_id: str, user_id: str, query: str, format: str) -> ExportResponse:
        # Move logic from ExportDataHandler
        pass
```

**Step 2: Test Each Endpoint**
```bash
# Test dataset download
curl -X GET "http://localhost:8000/api/v1/datasets/{dataset_id}/download?format=csv" \
  -H "Authorization: Bearer $TOKEN" \
  -o dataset.csv

# Test table download
curl -X GET "http://localhost:8000/api/v1/datasets/{dataset_id}/tables/{table_key}/download?format=parquet" \
  -H "Authorization: Bearer $TOKEN" \
  -o table.parquet

# Test data export
curl -X POST "http://localhost:8000/api/v1/datasets/{dataset_id}/export" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM data WHERE created > 2024-01-01", "format": "json"}'
```

#### 2.5 Exploration Feature

**Step 1: Create ExplorationService**
```python
# src/features/exploration/services.py
class ExplorationService(BaseService):
    @with_error_handling
    @with_transaction
    async def create_exploration_job(self, dataset_id: str, user_id: str, query: str, parameters: dict) -> ExplorationJobResponse:
        # Move logic from CreateExplorationJobHandler
        pass
    
    @with_error_handling
    async def get_exploration_results(self, job_id: str, user_id: str) -> ExplorationResultsResponse:
        # Move logic from GetExplorationResultsHandler
        pass
    
    @with_error_handling
    async def get_exploration_history(self, dataset_id: str, user_id: str) -> List[ExplorationJobResponse]:
        # Move logic from GetExplorationHistoryHandler
        pass
```

**Step 2: Test Each Endpoint**
```bash
# Test exploration job creation
curl -X POST "http://localhost:8000/api/v1/exploration/jobs" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"dataset_id": "123", "query": "SELECT * FROM data", "parameters": {}}'

# Test get results
curl -X GET "http://localhost:8000/api/v1/exploration/jobs/{job_id}/results" \
  -H "Authorization: Bearer $TOKEN"

# Test exploration history
curl -X GET "http://localhost:8000/api/v1/datasets/{dataset_id}/exploration-history" \
  -H "Authorization: Bearer $TOKEN"
```

#### 2.6 Sampling Feature

**Step 1: Create SamplingService**
```python
# src/features/sampling/services.py
class SamplingService(BaseService):
    @with_error_handling
    @with_transaction
    async def create_sample(self, dataset_id: str, user_id: str, method: str, size: int, config: dict) -> SampleResponse:
        # Move logic from CreateSampleHandler
        # CRITICAL: Fix async/sync mixing issues here
        pass
    
    @with_error_handling
    async def get_sample_data(self, sample_id: str, user_id: str) -> SampleDataResponse:
        # Move logic from GetSampleDataHandler
        pass
    
    @with_error_handling
    async def get_column_samples(self, dataset_id: str, column_name: str, user_id: str) -> ColumnSampleResponse:
        # Move logic from GetColumnSamplesHandler
        pass
```

**Step 2: Test Each Endpoint**
```bash
# Test sample creation
curl -X POST "http://localhost:8000/api/v1/datasets/{dataset_id}/samples" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"method": "random", "size": 1000, "config": {"seed": 42}}'

# Test get sample data
curl -X GET "http://localhost:8000/api/v1/samples/{sample_id}/data" \
  -H "Authorization: Bearer $TOKEN"

# Test column samples
curl -X GET "http://localhost:8000/api/v1/datasets/{dataset_id}/columns/{column_name}/samples" \
  -H "Authorization: Bearer $TOKEN"
```

#### 2.7 Search Feature

**Step 1: Create SearchService**
```python
# src/features/search/services.py
class SearchService(BaseService):
    @with_error_handling
    async def search_datasets(self, query: str, user_id: str, filters: dict = None) -> SearchResponse:
        # Move logic from SearchDatasetsHandler
        # IMPORTANT: Remove unnecessary transaction for read operation
        pass
    
    @with_error_handling
    async def get_suggestions(self, query: str, user_id: str) -> SuggestionsResponse:
        # Move logic from SuggestHandler
        pass
    
    @with_error_handling
    @with_transaction
    async def refresh_search_index(self, dataset_id: str, user_id: str) -> bool:
        # Move logic from RefreshSearchIndexHandler
        pass
```

**Step 2: Test Each Endpoint**
```bash
# Test dataset search
curl -X POST "http://localhost:8000/api/v1/search/datasets" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query": "sales data", "filters": {"created_after": "2024-01-01"}}'

# Test suggestions
curl -X GET "http://localhost:8000/api/v1/search/suggestions?q=sal" \
  -H "Authorization: Bearer $TOKEN"

# Test index refresh
curl -X POST "http://localhost:8000/api/v1/datasets/{dataset_id}/search-index/refresh" \
  -H "Authorization: Bearer $TOKEN"
```

#### 2.8 SQL Workbench Feature

**Step 1: Create SqlWorkbenchService**
```python
# src/features/sql_workbench/services.py
class SqlWorkbenchService(BaseService):
    @with_error_handling
    async def preview_sql(self, dataset_id: str, query: str, user_id: str, limit: int = 100) -> SqlPreviewResponse:
        # Move logic from PreviewSqlHandler
        # CRITICAL: Parameterize all queries to prevent SQL injection
        pass
    
    @with_error_handling
    @with_transaction
    async def execute_sql_transform(self, dataset_id: str, query: str, user_id: str, save_as: str) -> SqlTransformResponse:
        # Move logic from TransformSqlHandler
        pass
    
    @with_error_handling
    async def validate_sql(self, query: str, dataset_id: str, user_id: str) -> SqlValidationResponse:
        # Move logic from ValidateSqlHandler
        pass
```

**Step 2: Test Each Endpoint**
```bash
# Test SQL preview
curl -X POST "http://localhost:8000/api/v1/sql/preview" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"dataset_id": "123", "query": "SELECT * FROM data LIMIT 10"}'

# Test SQL transform
curl -X POST "http://localhost:8000/api/v1/sql/transform" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"dataset_id": "123", "query": "SELECT category, SUM(amount) FROM sales GROUP BY category", "save_as": "category_totals"}'

# Test SQL validation
curl -X POST "http://localhost:8000/api/v1/sql/validate" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"dataset_id": "123", "query": "SELECT * FORM users"}'  # Note: intentional typo to test validation
```

#### Critical Testing Protocol for Phase 2

**Before ANY refactoring:**
1. Create a test script with all curl commands for the feature
2. Run the script and save all responses:
   ```bash
   ./test_feature.sh > before_refactoring.log 2>&1
   ```

**After EACH handler migration:**
1. Run the same test script:
   ```bash
   ./test_feature.sh > after_handler_X.log 2>&1
   ```
2. Compare outputs:
   ```bash
   diff before_refactoring.log after_handler_X.log
   ```
3. If ANY differences found, STOP and fix before proceeding

**Common Issues to Watch For:**
- Missing authentication headers causing 401 errors
- Changed response format (missing fields, different structure)
- Performance regression (timeouts, slow responses)
- Missing events that were previously published
- Different error messages or status codes

### Phase 3: Update Workers (Week 6)

Transform workers from business logic containers to thin orchestrators.

#### 3.1 Import Worker Example

**Before:**
```python
# workers/import_executor.py
class ImportJobExecutor:
    def execute(self, job_id, parameters, db_pool):
        # Lots of business logic...
        # Direct database access...
        # File parsing logic...
```

**After:**
```python
# workers/dataset_tasks.py
from src.container import get_dataset_service
from src.infrastructure.postgres.database import get_session
from src.infrastructure.postgres.uow import PostgresUnitOfWork

async def import_dataset_task(job_id: str, file_path: str, user_id: str):
    """Thin orchestrator for dataset import."""
    async with get_session() as session:
        uow = PostgresUnitOfWork(session)
        dataset_service = get_dataset_service(uow)
        
        # All business logic is in the service
        await dataset_service.import_from_file(
            job_id=job_id,
            file_path=file_path,
            user_id=user_id
        )

# Celery task wrapper
@app.task
def import_dataset(job_id: str, file_path: str, user_id: str):
    import asyncio
    asyncio.run(import_dataset_task(job_id, file_path, user_id))
```

#### 3.2 Update All Workers
- `sampling_executor.py` → Uses `SamplingService`
- `exploration_executor.py` → Uses `ExplorationService`
- `sql_transform_executor.py` → Uses `SqlTransformService`

### Phase 4: Final Cleanup (Week 7)

1. **Delete obsolete code:**
   - `/src/services/` directory
   - All handler directories
   - Command/Query objects
   - `BaseHandler` classes

2. **Update configuration:**
   - Remove handler imports from `__init__.py` files
   - Update any remaining import statements

3. **Documentation:**
   - Update README with new architecture
   - Create developer guide for service pattern
   - Document dependency injection approach

### Phase 5: Testing and Validation (Week 8)

#### Test Structure
```
tests/
├── unit/
│   ├── features/
│   │   ├── datasets/
│   │   │   └── test_dataset_service.py
│   │   └── users/
│   │       └── test_user_service.py
│   └── ...
├── integration/
│   └── api/
│       ├── test_datasets_api.py
│       └── test_users_api.py
└── ...
```

#### Testing Protocol

**Before refactoring ANY handler:**
1. Run the endpoint and save response:
   ```bash
   curl -X GET "http://localhost:8000/api/endpoint" > before.json
   ```
2. Note response time, status code, headers

**After refactoring to service:**
1. Run exact same request:
   ```bash
   curl -X GET "http://localhost:8000/api/endpoint" > after.json
   ```
2. Compare: `diff before.json after.json`
3. If ANY difference, STOP and fix

**Red flags requiring immediate fix:**
- Different HTTP status codes
- Missing/extra fields in response
- Different error formats
- Response >2x slower
- Missing events that were published before

#### Specific Test Cases by Feature

**Users Feature:**
```bash
# Create user
curl -X POST "/api/v1/users" -d '{"email": "test@example.com", "password": "Test123!"}'

# Login
curl -X POST "/api/v1/users/login" -d '{"email": "test@example.com", "password": "Test123!"}'

# Update user
curl -X PUT "/api/v1/users/{id}" -H "Authorization: Bearer $TOKEN" -d '{"name": "New Name"}'

# Test validation errors
curl -X POST "/api/v1/users" -d '{"email": "invalid-email", "password": "short"}'
```

**Datasets Feature:**
```bash
# Create dataset
curl -X POST "/api/v1/datasets" -H "Authorization: Bearer $TOKEN" -d '{"name": "Test Dataset"}'

# List datasets
curl -X GET "/api/v1/datasets" -H "Authorization: Bearer $TOKEN"

# Download dataset
curl -X GET "/api/v1/datasets/{id}/download" -H "Authorization: Bearer $TOKEN"
```

**Jobs Feature:**
```bash
# Create job
curl -X POST "/api/v1/jobs" -H "Authorization: Bearer $TOKEN" -d '{"type": "import", "config": {}}'

# Check status
curl -X GET "/api/v1/jobs/{id}" -H "Authorization: Bearer $TOKEN"

# Cancel job
curl -X POST "/api/v1/jobs/{id}/cancel" -H "Authorization: Bearer $TOKEN"
```

## Success Metrics

### Code Quality Metrics
- ✓ 100% of handlers removed
- ✓ 0 Command/Query objects remain
- ✓ All business logic in services
- ✓ Services shared between API and workers
- ✓ Consistent error handling via decorators

### Architecture Metrics
- ✓ Single source of truth for service construction (container.py)
- ✓ Clear separation of concerns
- ✓ No business logic in API layer or workers
- ✓ Transaction boundaries properly managed
- ✓ Events collected and published after commit

### Developer Experience
- ✓ Simpler mental model (no CQRS confusion)
- ✓ Easy to find where logic lives (always in services)
- ✓ Consistent patterns across all features
- ✓ Easier testing (mock at service boundary)
- ✓ Clear dependency graph

## Risk Mitigation

### Risk 1: Breaking Changes
- **Mitigation**: Feature flags for gradual rollout
- **Mitigation**: Comprehensive test suite before starting
- **Mitigation**: One feature at a time
- **Mitigation**: Keep old handlers in separate branch for emergency rollback

### Risk 2: Performance Regression
- **Mitigation**: Benchmark critical paths before/after
- **Mitigation**: Keep transaction boundaries tight
- **Mitigation**: Profile service methods
- **Mitigation**: Address specific issues like async/sync mixing that cause blocking

### Risk 3: Team Adoption
- **Mitigation**: Pilot with volunteers
- **Mitigation**: Pair programming during migration
- **Mitigation**: Clear documentation and examples
- **Mitigation**: Show concrete benefits (simpler code, fewer bugs)

### Risk 4: Security Implications
Current inconsistencies create security risks that will be addressed:
- **Permission bypass risk**: Some handlers don't check permissions
- **SQL injection risk**: Raw SQL in some handlers
- **Logging sensitive data**: Passwords/tokens in logs
- **Mitigation**: Standardized permission checks in all services
- **Mitigation**: Parameterized queries only
- **Mitigation**: Audit logging practices

## Long-term Benefits

1. **Maintainability**: Clear, consistent patterns make changes easier
2. **Testability**: Business logic isolated from infrastructure
3. **Scalability**: Easy to split services into microservices later
4. **Onboarding**: New developers understand structure quickly
5. **Flexibility**: Can add GraphQL, gRPC, or other interfaces easily

## Migration Checklist

### Per-Feature Checklist
- [ ] Create service class inheriting from BaseService
- [ ] Move all handler logic to service methods
- [ ] Apply decorators (@with_error_handling, @with_transaction)
- [ ] Update models.py with Pydantic request/response models
- [ ] Update API router to call service directly
- [ ] Delete handlers directory
- [ ] Delete command objects
- [ ] Update unit tests for service
- [ ] Update integration tests for API
- [ ] Update documentation

### Global Checklist
- [ ] Create container.py with all service factories
- [ ] Create base_service.py
- [ ] Update core/decorators.py
- [ ] Enhance UoW with event collection
- [ ] Update all workers to use services
- [ ] Delete /src/services directory
- [ ] Delete base handler classes
- [ ] Update project documentation
- [ ] Team training completed

## Monitoring During Migration

### Key Metrics to Track
1. **API Response Times**: Ensure no performance regression
2. **Error Rates**: Monitor for increased errors during rollout
3. **Test Coverage**: Maintain or improve coverage
4. **Worker Task Completion**: Ensure background jobs still work
5. **Database Connection Pool**: Watch for exhaustion

### Rollback Strategy
- Feature flags for each migrated feature
- Keep old handlers in separate branch for 2 weeks
- Database migrations should be backward compatible
- Monitor error rates closely first 48 hours after each feature

## Conclusion

This refactoring transforms a complex, inconsistent codebase into a clean, maintainable architecture. By following this plan, you'll achieve:
- Consistent patterns across all features
- Clear separation of concerns
- Simplified mental model
- Better testing and maintainability
- Foundation for future growth

The investment of 8 weeks will pay dividends in development velocity, code quality, and team satisfaction.

## Appendix: Feature-Specific Refactoring Notes

### Downloads Feature
**Current Issues:**
- No base class inheritance
- Manual error handling
- No decorators used

**Refactoring Priority:** High - This feature has the most inconsistencies

### Exploration Feature  
**Current Issues:**
- Response models defined in handler files
- No base class inheritance
- Missing permission checks in some handlers

**Special Consideration:** Complex async operations need careful migration

### Sampling Feature
**Current Issues:**
- Dangerous async/sync mixing causing event loop blocking
- No base class inheritance
- Performance bottlenecks

**Critical Fix:** Must resolve sync operations in async context immediately

### SQL Workbench Feature
**Current Issues:**
- SQL injection risks from raw SQL construction
- Missing query timeouts
- Inconsistent decorator usage

**Security Priority:** Parameterize all queries during refactoring

### Search Feature
**Current Issues:**
- Unnecessary transactions for read operations (10-15% overhead)
- Manual transaction management despite using BaseHandler

**Performance Note:** Remove transactions from read-only operations

### Versioning Feature
**Current Issues:**
- Complex inter-dependencies
- Mixed handler patterns
- Many handlers to refactor

**Approach:** Start with simpler handlers (get operations) before complex ones (create commit)