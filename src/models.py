from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class SessionSummary(BaseModel):
    session_id: str
    start_time: datetime
    sport: str
    duration_minutes: float
    total_distance_km: float
    avg_heart_rate: Optional[float] = None
    calories: Optional[int] = None

class GlobalSummary(BaseModel):
    total_sessions: int
    total_distance_km: float
    total_duration_hours: float
    last_updated: datetime
