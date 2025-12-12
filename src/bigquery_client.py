from google.cloud import bigquery
from typing import List, Optional
import os
from .models import SessionSummary, GlobalSummary, SessionDetail, DailyActivitySummary, WeeklyActivitySummary, MonthlyActivitySummary
from datetime import datetime, date

class BigQueryClient:
    def __init__(self):
        self.project_id = os.getenv("BIGQUERY_PROJECT_ID")
        self.dataset_id = os.getenv("BIGQUERY_DATASET")
        self.client = bigquery.Client(project=self.project_id)

    def get_recent_sessions(self, limit: int = 10, offset: int = 0) -> List[SessionSummary]:
        # Select all columns defined in the SessionSummary model
        # Using * for simplicity, but explicit selection matches the model definition better
        # For this specific case, we'll explicitly map the query to the model fields
        query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset_id}.sessions`
            ORDER BY start_time DESC
            LIMIT @limit OFFSET @offset
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
                bigquery.ScalarQueryParameter("offset", "INT64", offset)
            ]
        )
        query_job = self.client.query(query, job_config=job_config)
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

    def get_session_by_id(self, session_id: str) -> Optional[SessionSummary]:
        query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset_id}.sessions`
            WHERE session_id = @session_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("session_id", "STRING", session_id)
            ]
        )
        query_job = self.client.query(query, job_config=job_config)
        results = query_job.result()
        
        for row in results:
            return SessionSummary(
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
            )
        return None

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

    def get_session_details(self, session_id: str) -> List[SessionDetail]:
        query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset_id}.details`
            WHERE session_id = @session_id
            ORDER BY timestamp ASC
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("session_id", "STRING", session_id)
            ]
        )
        query_job = self.client.query(query, job_config=job_config)
        results = query_job.result()
        
        details = []
        for row in results:
            details.append(SessionDetail(
                session_id=row.session_id,
                file_hash=row.file_hash,
                record_id=row.record_id,
                timestamp=row.timestamp,
                position_lat=row.position_lat,
                position_long=row.position_long,
                gps_accuracy=row.gps_accuracy,
                altitude=row.altitude,
                enhanced_altitude=row.enhanced_altitude,
                grade=row.grade,
                distance=row.distance,
                heart_rate=row.heart_rate,
                cadence=row.cadence,
                power=row.power,
                speed=row.speed,
                enhanced_speed=row.enhanced_speed,
                temperature=row.temperature,
                calories=row.calories,
                battery_soc=row.battery_soc
            ))
        return details

    def get_daily_activity_summary(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        sport: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[DailyActivitySummary]:
        base_query = f"""
            SELECT
                activity_date,
                sport,
                session_count,
                total_distance_m,
                total_elapsed_time
            FROM `{self.project_id}.{self.dataset_id}.daily_activity_summary_mv`
            WHERE 1=1
        """

        query_parameters = []

        if start_date is not None:
            base_query += " AND activity_date >= @start_date"
            query_parameters.append(
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date)
            )

        if end_date is not None:
            base_query += " AND activity_date <= @end_date"
            query_parameters.append(
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date)
            )

        if sport is not None:
            base_query += " AND sport = @sport"
            query_parameters.append(
                bigquery.ScalarQueryParameter("sport", "STRING", sport)
            )

        base_query += "\n ORDER BY activity_date DESC, sport ASC"

        if limit is not None:
            base_query += f"\n LIMIT {int(limit)}"
            if offset is not None and offset != 0:
                base_query += f" OFFSET {int(offset)}"

        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
        query_job = self.client.query(base_query, job_config=job_config)
        results = query_job.result()

        summaries: List[DailyActivitySummary] = []
        for row in results:
            summaries.append(
                DailyActivitySummary(
                    activity_date=row.activity_date,
                    sport=row.sport,
                    session_count=row.session_count,
                    total_distance_m=row.total_distance_m,
                    total_elapsed_time=row.total_elapsed_time,
                )
            )

        return summaries

    def get_weekly_activity_summary(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        sport: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[WeeklyActivitySummary]:
        base_query = f"""
            SELECT
                week_start_date,
                iso_year,
                iso_week,
                sport,
                session_count,
                total_distance_m,
                total_elapsed_time
            FROM `{self.project_id}.{self.dataset_id}.weekly_activity_summary_v`
            WHERE 1=1
        """

        query_parameters = []

        if start_date is not None:
            base_query += " AND week_start_date >= @start_date"
            query_parameters.append(
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date)
            )

        if end_date is not None:
            base_query += " AND week_start_date <= @end_date"
            query_parameters.append(
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date)
            )

        if sport is not None:
            base_query += " AND sport = @sport"
            query_parameters.append(
                bigquery.ScalarQueryParameter("sport", "STRING", sport)
            )

        base_query += "\n ORDER BY week_start_date DESC, sport ASC"

        if limit is not None:
            base_query += f"\n LIMIT {int(limit)}"
            if offset is not None and offset != 0:
                base_query += f" OFFSET {int(offset)}"

        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
        query_job = self.client.query(base_query, job_config=job_config)
        results = query_job.result()

        summaries: List[WeeklyActivitySummary] = []
        for row in results:
            summaries.append(
                WeeklyActivitySummary(
                    week_start_date=row.week_start_date,
                    iso_year=row.iso_year,
                    iso_week=row.iso_week,
                    sport=row.sport,
                    session_count=row.session_count,
                    total_distance_m=row.total_distance_m,
                    total_elapsed_time=row.total_elapsed_time,
                )
            )

        return summaries

    def get_monthly_activity_summary(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        sport: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[MonthlyActivitySummary]:
        base_query = f"""
            SELECT
                month_start_date,
                year,
                month,
                sport,
                session_count,
                total_distance_m,
                total_elapsed_time
            FROM `{self.project_id}.{self.dataset_id}.monthly_activity_summary_v`
            WHERE 1=1
        """

        query_parameters = []

        if start_date is not None:
            base_query += " AND month_start_date >= @start_date"
            query_parameters.append(
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date)
            )

        if end_date is not None:
            base_query += " AND month_start_date <= @end_date"
            query_parameters.append(
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date)
            )

        if sport is not None:
            base_query += " AND sport = @sport"
            query_parameters.append(
                bigquery.ScalarQueryParameter("sport", "STRING", sport)
            )

        base_query += "\n ORDER BY month_start_date DESC, sport ASC"

        if limit is not None:
            base_query += f"\n LIMIT {int(limit)}"
            if offset is not None and offset != 0:
                base_query += f" OFFSET {int(offset)}"

        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
        query_job = self.client.query(base_query, job_config=job_config)
        results = query_job.result()

        summaries: List[MonthlyActivitySummary] = []
        for row in results:
            summaries.append(
                MonthlyActivitySummary(
                    month_start_date=row.month_start_date,
                    year=row.year,
                    month=row.month,
                    sport=row.sport,
                    session_count=row.session_count,
                    total_distance_m=row.total_distance_m,
                    total_elapsed_time=row.total_elapsed_time,
                )
            )

        return summaries
