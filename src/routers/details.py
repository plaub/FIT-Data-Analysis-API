from fastapi import APIRouter, Depends
from fastapi_limiter.depends import RateLimiter
import json
from typing import List

from ..models import SessionDetail, ResponseWithSource
from ..config import settings, rate_limiter
from ..dependencies import get_redis, get_bq_client

router = APIRouter(
    prefix="/api/sessions",
    tags=["sessions"]
)

@router.get("/{session_id}/details", 
            response_model=ResponseWithSource[List[SessionDetail]], 
            dependencies=[Depends(RateLimiter(limiter=rate_limiter))])
async def get_session_details(
    session_id: str,
    fields: str = None, # Comma separated list of fields
    redis = Depends(get_redis),
    bq_client = Depends(get_bq_client)
):
    # Parse fields if provided
    field_list = [f.strip() for f in fields.split(",")] if fields else None
    
    # Cache Key is ALWAYS by session_id (we cache the full response)
    cache_key = f"session_details:{session_id}"
    
    # Helper to filter fields
    def filter_fields(details: List[SessionDetail], selected: List[str]) -> List[SessionDetail]:
        if not selected:
            return details
        
        # Ensure ID fields are always present
        required = {"session_id", "timestamp", "record_id", "file_hash"}
        keep = required.union(set(selected))
        
        filtered = []
        for d in details:
            # Create a copy with only selected fields, others None
            
            d_dict = d.model_dump()
            filtered_dict = {k: v for k, v in d_dict.items() if k in keep}
            filtered.append(SessionDetail(**filtered_dict))
            
        return filtered

    # Try Cache
    cached_data = await redis.get(cache_key)
    if cached_data:
        data_list = json.loads(cached_data)
        full_details = [SessionDetail(**item) for item in data_list]
        return ResponseWithSource(
            data=filter_fields(full_details, field_list),
            source="cache"
        )
    
    # Cache Miss - Get ALL details from BQ
    full_details = bq_client.get_session_details(session_id)
    
    # Serialize and Cache FULL details
    if full_details:
        details_json = [d.model_dump() for d in full_details]
        await redis.set(
            cache_key, 
            json.dumps(details_json, default=str), 
            ex=settings.CACHE_TTL_DETAILS
        )
    
    return ResponseWithSource(
        data=filter_fields(full_details, field_list),
        source="bigquery"
    )
