from fastapi import APIRouter, Depends
from fastapi_limiter.depends import RateLimiter
import json

from ..models import GlobalSummary
from ..config import settings
from ..dependencies import get_redis, get_bq_client

router = APIRouter(
    prefix="/api/summary",
    tags=["summary"]
)

@router.get("", 
            response_model=GlobalSummary, 
            dependencies=[Depends(RateLimiter(times=settings.RATE_LIMIT_PER_MINUTE, seconds=60))])

async def get_summary(
    redis = Depends(get_redis),
    bq_client = Depends(get_bq_client)
):
    cache_key = "global_summary"
    
    # Try Cache
    cached_data = await redis.get(cache_key)
    if cached_data:
        return GlobalSummary(**json.loads(cached_data))
    
    # Cache Miss
    summary = bq_client.get_global_summary()
    
    # Cache
    await redis.set(
        cache_key, 
        json.dumps(summary.model_dump(), default=str), 
        ex=settings.CACHE_TTL_SUMMARY
    )
    
    return summary
