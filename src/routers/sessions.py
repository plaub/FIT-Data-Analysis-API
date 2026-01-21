from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi_limiter.depends import RateLimiter
import json
from typing import List
from datetime import date

from ..models import SessionSummary, ResponseWithSource
from ..config import settings
from ..dependencies import get_redis, get_bq_client

router = APIRouter(
    prefix="/api/sessions",
    tags=["sessions"]
)

@router.get("", 
            response_model=ResponseWithSource[List[SessionSummary]], 
            dependencies=[Depends(RateLimiter(times=settings.RATE_LIMIT_PER_MINUTE, seconds=60))])

async def get_sessions(
    page: int = Query(1, ge=1, description="Page number"),
    sport: str = Query(None, description="Filter by sport"),
    start_date: date = Query(None, description="Filter by start date (YYYY-MM-DD)"),
    end_date: date = Query(None, description="Filter by end date (YYYY-MM-DD)"),
    min_distance: float = Query(None, description="Filter by minimum distance in meters"),
    max_distance: float = Query(None, description="Filter by maximum distance in meters"),
    redis = Depends(get_redis),
    bq_client = Depends(get_bq_client)
):
    limit = 10
    offset = (page - 1) * limit
    
    # Generate cache key based on all parameters
    cache_params = {
        "page": page,
        "sport": sport,
        "start_date": str(start_date) if start_date else None,
        "end_date": str(end_date) if end_date else None,
        "min_distance": min_distance,
        "max_distance": max_distance
    }
    cache_key = f"sessions_list_{json.dumps(cache_params, sort_keys=True)}"
    
    # Try Cache
    cached_data = await redis.get(cache_key)
    if cached_data:
        data_list = json.loads(cached_data)
        return ResponseWithSource(
            data=[SessionSummary(**item) for item in data_list],
            source="cache"
        )
    
    # Cache Miss
    sessions = bq_client.get_recent_sessions(
        limit=limit, 
        offset=offset,
        sport=sport,
        start_date=start_date,
        end_date=end_date,
        min_distance=min_distance,
        max_distance=max_distance
    )
    
    # Serialize and Cache
    sessions_json = [s.model_dump() for s in sessions]
    await redis.set(
        cache_key, 
        json.dumps(sessions_json, default=str), 
        ex=settings.CACHE_TTL_SESSIONS
    )
    
    return ResponseWithSource(
        data=sessions,
        source="bigquery"
    )


@router.get("/{session_id}",
            response_model=ResponseWithSource[SessionSummary],
            dependencies=[Depends(RateLimiter(times=settings.RATE_LIMIT_PER_MINUTE, seconds=60))])
async def get_session_by_id(
    session_id: str,
    redis = Depends(get_redis),
    bq_client = Depends(get_bq_client)
):
    cache_key = f"session_detail_{session_id}"
    
    # Try Cache
    cached_data = await redis.get(cache_key)
    if cached_data:
        data = json.loads(cached_data)
        return ResponseWithSource(
            data=SessionSummary(**data),
            source="cache"
        )
    
    # Cache Miss
    session = bq_client.get_session_by_id(session_id)
    
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found")
    
    # Serialize and Cache
    session_json = session.model_dump()
    await redis.set(
        cache_key,
        json.dumps(session_json, default=str),
        ex=settings.CACHE_TTL_SESSIONS
    )
    
    return ResponseWithSource(
        data=session,
        source="bigquery"
    )
