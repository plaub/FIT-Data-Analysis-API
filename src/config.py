import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # Redis
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
    REDIS_URL = os.getenv("REDIS_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}")

    # BigQuery
    BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
    BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "fitness_data")

    # Caching
    CACHE_TTL_SESSIONS = int(os.getenv("CACHE_TTL_SESSIONS", 604800))  # 1 Woche
    CACHE_TTL_SUMMARY = int(os.getenv("CACHE_TTL_SUMMARY", 604800))   # 1 Woche
    CACHE_TTL_DETAILS = int(os.getenv("CACHE_TTL_DETAILS", 604800)) # 1 Woche

    # Rate Limiting
    RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", 30))

    # CORS
    CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:4321,http://localhost:3000").split(",")

settings = Settings()
