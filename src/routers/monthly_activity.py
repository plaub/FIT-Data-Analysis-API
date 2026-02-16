from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from fastapi_limiter.depends import RateLimiter
import json

from ..models import MonthlyActivitySummary, ResponseWithSource
from ..config import settings, rate_limiter
from ..dependencies import get_redis, get_bq_client

router = APIRouter(
    prefix="/api/monthly-summary",
    tags=["summary"],
)


@router.get(
    "",
    response_model=ResponseWithSource[List[MonthlyActivitySummary]],
    dependencies=[Depends(RateLimiter(limiter=rate_limiter))],
)
async def get_monthly_activity_summary(
    start_date: Optional[date] = Query(
        None, description="Start date (inclusive, format YYYY-MM-DD)"
    ),
    end_date: Optional[date] = Query(
        None, description="End date (inclusive, format YYYY-MM-DD)"
    ),
    sport: Optional[str] = Query(None, description="Filter by sport"),
    redis=Depends(get_redis),
    bq_client=Depends(get_bq_client),
):
    cache_key = (
        f"monthly_activity:"  # base
        f"{start_date if start_date else 'none'}:"  # start
        f"{end_date if end_date else 'none'}:"  # end
        f"{sport if sport else 'none'}"  # sport
    )

    # Try Cache
    cached_data = await redis.get(cache_key)
    if cached_data:
        data_list = json.loads(cached_data)
        return ResponseWithSource(
            data=[MonthlyActivitySummary(**item) for item in data_list],
            source="cache"
        )

    # Cache Miss - query BigQuery
    summaries = bq_client.get_monthly_activity_summary(
        start_date=start_date,
        end_date=end_date,
        sport=sport,
    )

    # Serialize and cache
    if summaries:
        summaries_json = [s.model_dump() for s in summaries]
        await redis.set(
            cache_key,
            json.dumps(summaries_json, default=str),
            ex=getattr(settings, "CACHE_TTL_MONTHLY_ACTIVITY", settings.CACHE_TTL_SUMMARY),
        )

    return ResponseWithSource(
        data=summaries,
        source="bigquery"
    )
