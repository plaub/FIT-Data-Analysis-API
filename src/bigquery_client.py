from google.cloud import bigquery
from typing import List
import os
from .models import SessionSummary, GlobalSummary
from datetime import datetime

class BigQueryClient:
    def __init__(self):
        self.project_id = os.getenv("BIGQUERY_PROJECT_ID")
        self.dataset_id = os.getenv("BIGQUERY_DATASET")
        self.client = bigquery.Client(project=self.project_id)

    def get_recent_sessions(self, limit: int = 10) -> List[SessionSummary]:
        query = f"""
            SELECT 
                session_id,
                start_time,
                sport,
                total_timer_time / 60 as duration_minutes,
                total_distance / 1000 as total_distance_km,
                avg_heart_rate,
                total_calories as calories
            FROM `{self.project_id}.{self.dataset_id}.sessions`
            ORDER BY start_time DESC
            LIMIT {limit}
        """
        query_job = self.client.query(query)
        results = query_job.result()
        
        sessions = []
        for row in results:
            sessions.append(SessionSummary(
                session_id=row.session_id,
                start_time=row.start_time,
                sport=row.sport,
                duration_minutes=row.duration_minutes if row.duration_minutes else 0.0,
                total_distance_km=row.total_distance_km if row.total_distance_km else 0.0,
                avg_heart_rate=row.avg_heart_rate,
                calories=row.calories
            ))
        return sessions

    def get_global_summary(self) -> GlobalSummary:
        query = f"""
            SELECT
                COUNT(*) as total_sessions,
                SUM(total_distance) / 1000 as total_distance_km,
                SUM(total_timer_time) / 3600 as total_duration_hours
            FROM `{self.project_id}.{self.dataset_id}.sessions`
        """
        query_job = self.client.query(query)
        result = next(query_job.result())
        
        return GlobalSummary(
            total_sessions=result.total_sessions,
            total_distance_km=result.total_distance_km if result.total_distance_km else 0.0,
            total_duration_hours=result.total_duration_hours if result.total_duration_hours else 0.0,
            last_updated=datetime.now()
        )
