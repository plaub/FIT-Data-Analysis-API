from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from fastapi_limiter.depends import RateLimiter
import json

from ..models import DailyMetrics, ResponseWithSource
from ..config import settings
from ..dependencies import get_redis, get_bq_client

router = APIRouter(
    prefix="/api/daily-metrics",
    tags=["metrics"],
)


@router.get(
    "",
    response_model=ResponseWithSource[List[DailyMetrics]],
    dependencies=[Depends(RateLimiter(times=settings.RATE_LIMIT_PER_MINUTE, seconds=60))],
)
async def get_daily_metrics(
    start_date: Optional[date] = Query(
        None, description="Start date (inclusive, format YYYY-MM-DD)"
    ),
    end_date: Optional[date] = Query(
        None, description="End date (inclusive, format YYYY-MM-DD)"
    ),
    redis=Depends(get_redis),
    bq_client=Depends(get_bq_client),
):
    cache_key = (
        f"daily_metrics:"  # base
        f"{start_date if start_date else 'none'}:"  # start
        f"{end_date if end_date else 'none'}"  # end
    )

    # Try Cache
    cached_data = await redis.get(cache_key)
    if cached_data:
        data_list = json.loads(cached_data)
        return ResponseWithSource(
            data=[DailyMetrics(**item) for item in data_list],
            source="cache"
        )

    # Cache Miss - query BigQuery
    metrics = bq_client.get_daily_metrics(
        start_date=start_date,
        end_date=end_date,
    )

    # Serialize and cache
    if metrics:
        metrics_json = [m.model_dump() for m in metrics]
        await redis.set(
            cache_key,
            json.dumps(metrics_json, default=str),
            ex=settings.CACHE_TTL_METRICS,
        )

    return ResponseWithSource(
        data=metrics,
        source="bigquery"
    )
