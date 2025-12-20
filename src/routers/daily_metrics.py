from datetime import date, timedelta, datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from fastapi_limiter.depends import RateLimiter
import json

from ..models import DailyMetrics, MetricsSummary, ResponseWithSource
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

    # Fill gaps if range is specified
    if start_date and end_date:
        # Identify which dates have data
        existing_dates = {
            m.timestamp.date() if isinstance(m.timestamp, datetime) else m.timestamp 
            for m in metrics
        }
        
        filled_metrics = list(metrics)
        current_date = start_date
        while current_date <= end_date:
            if current_date not in existing_dates:
                # Create a placeholder entry with zeros
                filled_metrics.append(DailyMetrics(
                    file_hash="none",
                    filename="none",
                    timestamp=datetime(current_date.year, current_date.month, current_date.day, tzinfo=timezone.utc),
                    body_battery_min=0,
                    body_battery_max=0,
                    body_battery_avg=0,
                    pulse=0,
                    sleep_hours=0,
                    stress_level_max=0,
                    stress_level_avg=0,
                    time_awake=0,
                    time_in_deep_sleep=0,
                    time_in_light_sleep=0,
                    time_in_rem_sleep=0,
                    weight_kilograms=0,
                    created_at=datetime.now(timezone.utc)
                ))
            current_date += timedelta(days=1)
        
        # Sort by timestamp DESC (to match existing behavior)
        # We ensure consistency by converting everything to UTC for comparison
        metrics = sorted(
            filled_metrics, 
            key=lambda x: x.timestamp.astimezone(timezone.utc) if x.timestamp.tzinfo else x.timestamp.replace(tzinfo=timezone.utc), 
            reverse=True
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


@router.get(
    "/summary",
    response_model=ResponseWithSource[MetricsSummary],
    dependencies=[Depends(RateLimiter(times=settings.RATE_LIMIT_PER_MINUTE, seconds=60))],
)
async def get_metrics_summary(
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
        f"daily_metrics_summary:"  # base
        f"{start_date if start_date else 'none'}:"  # start
        f"{end_date if end_date else 'none'}"  # end
    )

    # Try Cache
    cached_data = await redis.get(cache_key)
    if cached_data:
        data_dict = json.loads(cached_data)
        return ResponseWithSource(
            data=MetricsSummary(**data_dict),
            source="cache"
        )

    # Cache Miss - query BigQuery
    summary = bq_client.get_metrics_summary(
        start_date=start_date,
        end_date=end_date,
    )

    # Serialize and cache
    if summary:
        summary_json = summary.model_dump()
        await redis.set(
            cache_key,
            json.dumps(summary_json, default=str),
            ex=settings.CACHE_TTL_METRICS,
        )

    return ResponseWithSource(
        data=summary,
        source="bigquery"
    )
