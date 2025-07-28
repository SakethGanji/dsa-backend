# Services Cleanup Plan

## Overview
This document outlines a comprehensive plan to clean up and refactor the services layer in the DSA project. The goal is to eliminate duplication, improve code organization, and create a more maintainable architecture.

## 1. SQL Workbench Services Consolidation

### Current Issues
- `sql_workbench_service.py` and `workbench_service.py` have overlapping functionality
- Both services handle SQL transformations and previews
- Duplicate implementation of `_execute_sql_with_sources` method
- Unclear separation of concerns between the two services

### Action Items
1. **Merge Services**
   - Keep `sql_workbench_service.py` as the main service
   - Extract reusable SQL execution logic into a separate utility class
   - Remove `workbench_service.py` after migrating all unique functionality

2. **Create SQL Execution Utility**
   - Create `src/features/sql_workbench/utils/sql_executor.py`
   - Move `_execute_sql_with_sources` to this utility
   - Share between all services that need SQL execution

3. **Remove Dead Code**
   - Remove `preview_transformation()` method that raises NotImplementedError
   - Remove unused `apply_transformation()` method
   - Remove placeholder `QueryOptimizationService` or implement it properly

## 2. SQL Validation Consolidation

### Current Issues
- SQL validation logic is duplicated across multiple services
- `SqlValidationService` in `sql_execution.py`
- `validate_transformation()` in `workbench_service.py`
- Different validation rules in different places

### Action Items
1. **Create Unified Validation Service**
   - Create `src/features/sql_workbench/services/sql_validator.py`
   - Consolidate all validation logic into one place
   - Define consistent validation rules
   - Support different validation levels (syntax, semantic, security)

2. **Validation Rules to Implement**
   - Disallowed keywords (DROP, CREATE, ALTER, etc.)
   - Syntax validation
   - Table reference validation
   - Resource usage estimation
   - Security checks

## 3. Empty __init__.py Files

### Current Issues
- Multiple empty `__init__.py` files in service directories
- Missing exports make imports more verbose
- No clear API surface for each service module

### Action Items
1. **Add Exports to Each __init__.py**
   ```python
   # Example for src/features/table_analysis/services/__init__.py
   from .table_analysis import TableAnalysisService
   
   __all__ = ['TableAnalysisService']
   ```

2. **Services to Update**
   - `/src/features/table_analysis/services/__init__.py`
   - `/src/features/statistics/services/__init__.py`
   - `/src/features/file_processing/services/__init__.py`

## 4. Service Architecture Improvements

### Current Issues
- Inconsistent service initialization patterns
- Mixed responsibilities in some services
- Tight coupling between services and infrastructure

### Action Items
1. **Standardize Service Pattern**
   - All services should follow dependency injection
   - Clear separation between business logic and infrastructure
   - Consistent error handling patterns

2. **Create Base Service Class**
   ```python
   # src/core/services/base_service.py
   class BaseService:
       def __init__(self, uow, permissions, logger):
           self._uow = uow
           self._permissions = permissions
           self._logger = logger
   ```

3. **Implement Service Registry**
   - Central place to register and retrieve services
   - Helps with dependency management
   - Enables easier testing

## 5. Data Export Services Unification

### Current Issues
- `data_export_service.py` and `download_service.py` might have overlapping functionality
- Multiple services handling similar export operations

### Action Items
1. **Analyze Export Services**
   - Review both services for overlapping functionality
   - Identify unique features in each
   - Plan consolidation strategy

2. **Create Unified Export Service**
   - Single service for all export operations
   - Support multiple export formats
   - Consistent API for all export types

## 6. Implementation Priority

### Phase 1: Critical Cleanup (Week 1)
1. Consolidate SQL workbench services
2. Create SQL execution utility
3. Remove dead code

### Phase 2: Validation & Standards (Week 2)
1. Create unified SQL validation service
2. Standardize service patterns
3. Update empty __init__.py files

### Phase 3: Architecture Improvements (Week 3)
1. Implement base service class
2. Create service registry
3. Unify data export services

### Phase 4: Testing & Documentation (Week 4)
1. Add comprehensive tests for refactored services
2. Update documentation
3. Create service architecture guide

## 7. Migration Strategy

### For Each Service Refactoring
1. **Create New Implementation**
   - Build new service alongside old one
   - Ensure feature parity
   - Add comprehensive tests

2. **Gradual Migration**
   - Update endpoints one by one
   - Run both services in parallel initially
   - Monitor for issues

3. **Cleanup**
   - Remove old service once migration is complete
   - Update all imports
   - Remove deprecated code

## 8. Testing Requirements

### Unit Tests
- Each service should have >80% coverage
- Test all validation rules
- Test error conditions

### Integration Tests
- Test service interactions
- Test database operations
- Test permission checks

### Performance Tests
- Benchmark SQL execution
- Test with large datasets
- Monitor memory usage

## 9. Documentation Updates

### Service Documentation
- Document each service's responsibilities
- API documentation for all public methods
- Usage examples

### Architecture Documentation
- Update system architecture diagrams
- Document service interactions
- Create service dependency graph

## 10. Success Metrics

### Code Quality
- Reduced code duplication by >50%
- Improved test coverage to >80%
- Consistent code style across all services

### Performance
- No regression in API response times
- Reduced memory usage for SQL operations
- Faster test execution

### Maintainability
- Clear service boundaries
- Easy to add new features
- Simplified debugging

## Notes

### Risks
- Breaking existing functionality during refactoring
- Performance regression
- Missing edge cases

### Mitigation
- Comprehensive testing before each phase
- Feature flags for gradual rollout
- Monitoring and rollback plans

### Dependencies
- Need to coordinate with frontend team for API changes
- Database schema might need updates
- CI/CD pipeline updates for new test requirements