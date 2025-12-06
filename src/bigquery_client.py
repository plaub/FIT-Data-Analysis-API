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
        # Select all columns defined in the SessionSummary model
        # Using * for simplicity, but explicit selection matches the model definition better
        # For this specific case, we'll explicitly map the query to the model fields
        query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset_id}.sessions`
            ORDER BY start_time DESC
            LIMIT {limit}
        """
        query_job = self.client.query(query)
        results = query_job.result()
        
        sessions = []
        for row in results:
            # Map BigQuery row to SessionSummary model
            # Assuming row has attributes matching the model fields since we select *
            # and the model is based on the schema
            sessions.append(SessionSummary(
                file_hash=row.file_hash,
                filename=row.filename,
                session_id=row.session_id,
                timestamp=row.timestamp,
                start_time=row.start_time,
                manufacturer=row.manufacturer,
                product=row.product,
                serial_number=row.serial_number,
                sport=row.sport,
                sub_sport=row.sub_sport,
                total_elapsed_time=row.total_elapsed_time,
                total_timer_time=row.total_timer_time,
                total_distance=row.total_distance,
                avg_speed=row.avg_speed,
                max_speed=row.max_speed,
                avg_cadence=row.avg_cadence,
                max_cadence=row.max_cadence,
                min_heart_rate=row.min_heart_rate,
                avg_heart_rate=row.avg_heart_rate,
                max_heart_rate=row.max_heart_rate,
                avg_power=row.avg_power,
                max_power=row.max_power,
                normalized_power=row.normalized_power,
                threshold_power=row.threshold_power,
                total_work=row.total_work,
                total_calories=row.total_calories,
                min_altitude=row.min_altitude,
                avg_altitude=row.avg_altitude,
                max_altitude=row.max_altitude,
                total_ascent=row.total_ascent,
                total_descent=row.total_descent,
                avg_grade=row.avg_grade,
                max_pos_grade=row.max_pos_grade,
                max_neg_grade=row.max_neg_grade,
                avg_temperature=row.avg_temperature,
                max_temperature=row.max_temperature,
                training_stress_score=row.training_stress_score,
                intensity_factor=row.intensity_factor,
                num_laps=row.num_laps,
                created_at=row.created_at
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
