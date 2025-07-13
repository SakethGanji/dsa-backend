# Service Layer Refactoring Plan

## Issues Found

1. **Direct Infrastructure Dependencies**
   - `PostgresSamplingService` imports `DatabasePool` from infrastructure
   - `SamplingJobService` uses `DatabasePool` directly
   - `TableAnalyzer` imports `DatabasePool` from infrastructure

2. **Improper Separation of Concerns**
   - `SamplingJobService` is doing repository work (direct SQL queries)
   - Services should work through repositories and unit of work

## Refactoring Steps

### 1. Fix PostgresSamplingService
- Remove `DatabasePool` dependency
- Use `IUnitOfWork` and repositories instead
- Move job creation logic to job repository

### 2. Fix SamplingJobService
- This should not exist as a separate service
- Job management should be through `IJobRepository`
- Sampling service should use unit of work pattern

### 3. Fix TableAnalyzer
- Remove direct database dependency
- Work through abstractions (ITableReader, IUnitOfWork)
- Consider if this should be a service or part of import process

### 4. Create Proper Service Interfaces
- Ensure all services have interfaces in abstractions
- Services should only depend on abstractions
- No direct infrastructure imports

## Clean Architecture Rules for Services

1. Services should only depend on:
   - Core abstractions (interfaces)
   - Domain models
   - Other services (through interfaces)

2. Services should NOT depend on:
   - Infrastructure (database, external APIs)
   - Frameworks (FastAPI, etc.)
   - Concrete implementations

3. Services should use:
   - Unit of Work pattern for transactions
   - Repositories for data access
   - Dependency injection for flexibility