import os
from dotenv import load_dotenv

load_dotenv()

SECRET_KEY = os.getenv("SECRET_KEY", "super-secret")
REFRESH_SECRET = os.getenv("REFRESH_SECRET", "super-refresh-secret")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 15
REFRESH_TOKEN_EXPIRE_DAYS = 7

# Background job configuration
BACKGROUND_JOBS_ENABLED = os.getenv("BACKGROUND_JOBS_ENABLED", "true").lower() == "true"

VERSIONING_BACKGROUND_CONFIG = {
    "materialization_interval_minutes": int(os.getenv("MATERIALIZATION_INTERVAL_MINUTES", "60")),
    "gc_interval_minutes": int(os.getenv("GC_INTERVAL_MINUTES", "1440")),
    "materialization": {
        "max_concurrent_jobs": int(os.getenv("MAX_CONCURRENT_MATERIALIZATION_JOBS", "3")),
        "materialization_threshold": int(os.getenv("MATERIALIZATION_THRESHOLD", "5")),
        "age_threshold_hours": int(os.getenv("MATERIALIZATION_AGE_THRESHOLD_HOURS", "24")),
        "batch_size": int(os.getenv("MATERIALIZATION_BATCH_SIZE", "10"))
    },
    "gc": {
        "grace_period_hours": int(os.getenv("GC_GRACE_PERIOD_HOURS", "72")),
        "batch_size": int(os.getenv("GC_BATCH_SIZE", "100")),
        "dry_run": os.getenv("GC_DRY_RUN", "true").lower() == "true"
    }
}
