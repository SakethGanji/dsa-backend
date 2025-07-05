"""
Core service interfaces - re-export from abstractions for backward compatibility.

This file maintains the original interface paths while the actual interfaces
are now organized in core.abstractions for better architecture.
"""

# Re-export all interfaces from abstractions
from ..abstractions import *

# For backward compatibility, ensure all original exports are available
__all__ = [
    # Unit of Work
    "IUnitOfWork",
    # Repositories
    "IUserRepository", 
    "IDatasetRepository",
    "ICommitRepository",
    "IJobRepository",
    "ITableReader",
    # Services
    "IFileProcessingService",
    "IStatisticsService",
    "IExplorationService",
    "ISamplingService",
    "IWorkbenchService",
]