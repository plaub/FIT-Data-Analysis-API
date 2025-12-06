from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class SessionSummary(BaseModel):
    file_hash: str
    filename: str
    session_id: str
    timestamp: Optional[datetime] = None
    start_time: Optional[datetime] = None
    manufacturer: Optional[str] = None
    product: Optional[str] = None
    serial_number: Optional[int] = None
    sport: Optional[str] = None
    sub_sport: Optional[str] = None
    total_elapsed_time: Optional[float] = None
    total_timer_time: Optional[float] = None
    total_distance: Optional[float] = None
    avg_speed: Optional[float] = None
    max_speed: Optional[float] = None
    avg_cadence: Optional[int] = None
    max_cadence: Optional[int] = None
    min_heart_rate: Optional[int] = None
    avg_heart_rate: Optional[int] = None
    max_heart_rate: Optional[int] = None
    avg_power: Optional[int] = None
    max_power: Optional[int] = None
    normalized_power: Optional[int] = None
    threshold_power: Optional[int] = None
    total_work: Optional[int] = None
    total_calories: Optional[int] = None
    min_altitude: Optional[float] = None
    avg_altitude: Optional[float] = None
    max_altitude: Optional[float] = None
    total_ascent: Optional[int] = None
    total_descent: Optional[int] = None
    avg_grade: Optional[float] = None
    max_pos_grade: Optional[float] = None
    max_neg_grade: Optional[float] = None
    avg_temperature: Optional[int] = None
    max_temperature: Optional[int] = None
    training_stress_score: Optional[float] = None
    intensity_factor: Optional[float] = None
    num_laps: Optional[int] = None
    created_at: datetime

class GlobalSummary(BaseModel):
    total_sessions: int
    total_distance_km: float
    total_duration_hours: float
    last_updated: datetime
