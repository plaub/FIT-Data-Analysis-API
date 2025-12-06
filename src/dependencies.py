from fastapi import Request
from .config import settings
from .bigquery_client import BigQueryClient

# Initialize BigQuery Client (Singleton-ish)
bq_client = BigQueryClient()

def get_bq_client() -> BigQueryClient:
    return bq_client

def get_redis(request: Request):
    return request.app.state.redis
