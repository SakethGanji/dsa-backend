"""Run the job worker."""

import asyncio
import logging
import sys
from pathlib import Path
from urllib.parse import quote_plus, urlparse, urlunparse

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.infrastructure.config import get_settings
from src.infrastructure.postgres.database import DatabasePool
from src.infrastructure.external.password_manager import get_password_manager
from src.workers.job_worker import JobWorker
from src.workers.import_executor import ImportJobExecutor
from src.workers.sampling_executor import SamplingJobExecutor
from src.workers.exploration_executor import ExplorationExecutor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def main():
    """Main function to run the worker."""
    settings = get_settings()
    
    # Initialize database pool with dynamic password if configured
    dsn = f"postgresql://{settings.POSTGRESQL_USER}:{settings.POSTGRESQL_PASSWORD}@{settings.POSTGRESQL_HOST}:{settings.POSTGRESQL_PORT}/{settings.POSTGRESQL_DATABASE}"
    if settings.POSTGRESQL_PASSWORD_SECRET_NAME:
        try:
            password_manager = get_password_manager()
            password = password_manager.postgresql_fetch()
            if password:
                # Parse and rebuild DSN with new password
                parsed = urlparse(dsn)
                netloc = f"{parsed.username}:{quote_plus(password)}@{parsed.hostname}:{parsed.port}"
                dsn = urlunparse((
                    parsed.scheme,
                    netloc,
                    parsed.path,
                    parsed.params,
                    parsed.query,
                    parsed.fragment
                ))
                logger.info("Using dynamic password from secret manager")
        except Exception as e:
            logger.error(f"Failed to fetch dynamic password: {e}")
            logger.info("Falling back to environment variable password")
    
    db_pool = DatabasePool(dsn)
    await db_pool.initialize()
    logger.info("Database pool initialized")
    
    # Create worker
    worker = JobWorker(db_pool)
    
    # Register executors
    worker.register_executor('import', ImportJobExecutor())
    worker.register_executor('sampling', SamplingJobExecutor())
    worker.register_executor('exploration', ExplorationExecutor(db_pool))
    
    # Start worker
    try:
        logger.info("Starting job worker...")
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await worker.stop()
        await db_pool.close()
        logger.info("Worker stopped")


if __name__ == "__main__":
    asyncio.run(main())