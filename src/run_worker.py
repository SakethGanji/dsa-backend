"""Run the job worker."""

import asyncio
import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config import get_settings
from src.core.database import DatabasePool
from src.workers.job_worker import JobWorker
from src.workers.import_executor import ImportJobExecutor
from src.workers.sampling_executor import SamplingJobExecutor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def main():
    """Main function to run the worker."""
    settings = get_settings()
    
    # Initialize database pool
    db_pool = DatabasePool(settings.database_url)
    await db_pool.initialize()
    logger.info("Database pool initialized")
    
    # Create worker
    worker = JobWorker(db_pool)
    
    # Register executors
    worker.register_executor('import', ImportJobExecutor())
    worker.register_executor('sampling', SamplingJobExecutor())
    
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