import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # Redis
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
    REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}"

    # BigQuery
    BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
    BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "fitness_data")

    # Caching
    CACHE_TTL_SESSIONS = int(os.getenv("CACHE_TTL_SESSIONS", 300))
    CACHE_TTL_SUMMARY = int(os.getenv("CACHE_TTL_SUMMARY", 3600))

    # Rate Limiting
    RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", 30))

settings = Settings()
