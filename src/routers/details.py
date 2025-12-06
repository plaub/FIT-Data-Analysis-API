from fastapi import APIRouter, Depends
from fastapi_limiter.depends import RateLimiter
import json
from typing import List

from ..models import SessionDetail
from ..config import settings
from ..dependencies import get_redis, get_bq_client

router = APIRouter(
    prefix="/api/sessions",
    tags=["sessions"]
)

@router.get("/{session_id}/details", 
            response_model=List[SessionDetail], 
            dependencies=[Depends(RateLimiter(times=settings.RATE_LIMIT_PER_MINUTE, seconds=60))])
async def get_session_details(
    session_id: str,
    redis = Depends(get_redis),
    bq_client = Depends(get_bq_client)
):
    cache_key = f"session_details:{session_id}"
    
    # Try Cache
    cached_data = await redis.get(cache_key)
    if cached_data:
        data_list = json.loads(cached_data)
        return [SessionDetail(**item) for item in data_list]
    
    # Cache Miss
    details = bq_client.get_session_details(session_id)
    
    # Serialize and Cache
    if details:
        details_json = [d.model_dump() for d in details]
        await redis.set(
            cache_key, 
            json.dumps(details_json, default=str), 
            ex=settings.CACHE_TTL_DETAILS
        )
    
    return details
