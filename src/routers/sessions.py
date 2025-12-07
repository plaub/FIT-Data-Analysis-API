from fastapi import APIRouter, Depends, Query
from fastapi_limiter.depends import RateLimiter
import json
from typing import List

from ..models import SessionSummary
from ..config import settings
from ..dependencies import get_redis, get_bq_client

router = APIRouter(
    prefix="/api/sessions",
    tags=["sessions"]
)

@router.get("", 
            response_model=List[SessionSummary], 
            dependencies=[Depends(RateLimiter(times=settings.RATE_LIMIT_PER_MINUTE, seconds=60))])

async def get_sessions(
    page: int = Query(1, ge=1, description="Page number"),
    redis = Depends(get_redis),
    bq_client = Depends(get_bq_client)
):
    limit = 10
    offset = (page - 1) * limit
    cache_key = f"sessions_list_page_{page}"
    
    # Try Cache
    cached_data = await redis.get(cache_key)
    if cached_data:
        data_list = json.loads(cached_data)
        return [SessionSummary(**item) for item in data_list]
    
    # Cache Miss
    sessions = bq_client.get_recent_sessions(limit=limit, offset=offset)
    
    # Serialize and Cache
    sessions_json = [s.model_dump() for s in sessions]
    await redis.set(
        cache_key, 
        json.dumps(sessions_json, default=str), 
        ex=settings.CACHE_TTL_SESSIONS
    )
    
    return sessions
