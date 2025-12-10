import pytest
from unittest.mock import MagicMock
from src.models import SessionSummary, GlobalSummary, SessionDetail, DailyActivitySummary
from datetime import datetime
import json

@pytest.mark.asyncio
async def test_health(client):
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

@pytest.mark.asyncio
async def test_get_sessions_no_cache(client, mock_bq_client, mock_redis):
    # Setup BigQuery Mock return - using required fields from new model
    mock_sessions = [
        SessionSummary(
            file_hash="hash123",
            filename="run.fit",
            session_id="1",
            created_at=datetime(2023, 1, 1, 10, 0, 0),
            
            # Optional fields
            start_time=datetime(2023, 1, 1, 10, 0, 0),
            sport="Running",
            total_timer_time=1800.0, # 30 mins * 60
            total_distance=5000.0   # 5 km * 1000
        )
    ]
    mock_bq_client.get_recent_sessions.return_value = mock_sessions
    
    # Run Request
    response = await client.get("/api/sessions")
    
    # Verify basics
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["session_id"] == "1"
    assert data[0]["file_hash"] == "hash123"
    
    # Verify Interactions
    mock_bq_client.get_recent_sessions.assert_called_once()  # Should hit DB
    mock_redis.get.assert_called_with("sessions_list_page_1")     # Should check cache
    mock_redis.set.assert_called_once()                    # Should set cache

@pytest.mark.asyncio
async def test_get_sessions_cached(client, mock_bq_client, mock_redis):
    # Setup Redis Mock to return cached data
    # Must match structure of SessionSummary model dump
    cached_data = [
        {
            "file_hash": "hash_cached",
            "filename": "cached.fit",
            "session_id": "cached_1", 
            "created_at": "2023-01-01T10:00:00",
            "start_time": "2023-01-01T10:00:00", 
            "sport": "Cycling", 
            "total_timer_time": 3600.0, 
            "total_distance": 20000.0
        }
    ]
    mock_redis.get.return_value = json.dumps(cached_data)
    
    # Run Request
    response = await client.get("/api/sessions")
    
    # Verify basics
    assert response.status_code == 200
    data = response.json()
    assert data[0]["session_id"] == "cached_1"
    
    # Verify Interactions
    mock_bq_client.get_recent_sessions.assert_not_called() # Should NOT hit DB
    mock_redis.get.assert_called_with("sessions_list_page_1")

@pytest.mark.asyncio
async def test_get_sessions_pagination(client, mock_bq_client, mock_redis):
    # Setup BigQuery Mock return
    mock_sessions = [
        SessionSummary(
            file_hash="hash_p2",
            filename="run_p2.fit",
            session_id="p2",
            created_at=datetime(2023, 1, 1, 10, 0, 0),
            start_time=datetime(2023, 1, 1, 10, 0, 0),
            sport="Running",
            total_timer_time=1800.0,
            total_distance=5000.0
        )
    ]
    mock_bq_client.get_recent_sessions.return_value = mock_sessions
    
    # Run Request for Page 2
    response = await client.get("/api/sessions?page=2")
    
    # Verify basics
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["session_id"] == "p2"
    
    # Verify Interactions
    # Offset should be (2-1)*10 = 10
    mock_bq_client.get_recent_sessions.assert_called_with(limit=10, offset=10)
    mock_redis.get.assert_called_with("sessions_list_page_2")


@pytest.mark.asyncio
async def test_get_summary(client, mock_bq_client, mock_redis):
    mock_summary = GlobalSummary(
        total_sessions=100,
        total_distance_km=500.0,
        total_duration_hours=50.0,
        last_updated=datetime(2023, 1, 1, 12, 0, 0)
    )
    mock_bq_client.get_global_summary.return_value = mock_summary
    
    response = await client.get("/api/summary")
    
    assert response.status_code == 200
    assert response.json()["total_sessions"] == 100
    mock_bq_client.get_global_summary.assert_called_once()

@pytest.mark.asyncio
async def test_get_session_details(client, mock_bq_client, mock_redis):
    session_id = "test_session_1"
    mock_details = [
        SessionDetail(
            session_id=session_id,
            file_hash="hash123",
            record_id="rec1",
            timestamp=datetime(2023, 1, 1, 10, 0, 0),
            heart_rate=140,
            power=200
        ),
        SessionDetail(
            session_id=session_id,
            file_hash="hash123",
            record_id="rec2",
            timestamp=datetime(2023, 1, 1, 10, 0, 5),
            heart_rate=145,
            power=210
        )
    ]
    mock_bq_client.get_session_details.return_value = mock_details
    
    response = await client.get(f"/api/sessions/{session_id}/details")
    
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["record_id"] == "rec1"
    assert data[1]["record_id"] == "rec2"
    
    mock_bq_client.get_session_details.assert_called_with(session_id)
    mock_redis.get.assert_called_with(f"session_details:{session_id}")

@pytest.mark.asyncio
async def test_get_session_details_with_fields(client, mock_bq_client, mock_redis):
    session_id = "test_session_1"
    # The mock returns full objects, API filters them
    mock_details = [
        SessionDetail(
            session_id=session_id,
            file_hash="hash123",
            record_id="rec1",
            timestamp=datetime(2023, 1, 1, 10, 0, 0),
            heart_rate=140,
            power=200 # This should be filtered out
        )
    ]
    mock_bq_client.get_session_details.return_value = mock_details
    
    fields_param = "heart_rate"
    response = await client.get(f"/api/sessions/{session_id}/details?fields={fields_param}")
    
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["heart_rate"] == 140
    # Power should not be in the response (or None if default, but we filtered)
    # The filter logic in router reconstructs the model, so unset fields are null/excluded depending on model dump config.
    # Our filter uses "keep" set.
    # We constructed the dict with valid keys. Power was not in keep.
    # So power should be None in the response object (since it's Optional in model).
    assert data[0]["power"] is None
    
    # Check that client was called without specific fields (always full fetch)
    mock_bq_client.get_session_details.assert_called_with(session_id)
    
    # Check cache key is the FULL one
    mock_redis.get.assert_called_with(f"session_details:{session_id}")


@pytest.mark.asyncio
async def test_get_daily_summary_no_cache(client, mock_bq_client, mock_redis):
    # Setup BigQuery Mock return
    mock_summaries = [
        DailyActivitySummary(
            activity_date=datetime(2023, 1, 1).date(),
            sport="Running",
            session_count=3,
            total_distance_m=15000.0,
            total_elapsed_time=5400.0,
        )
    ]

    mock_bq_client.get_daily_activity_summary.return_value = mock_summaries

    response = await client.get("/api/daily-summary?start_date=2023-01-01&end_date=2023-01-31&sport=Running")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["sport"] == "Running"
    assert data[0]["session_count"] == 3

    mock_bq_client.get_daily_activity_summary.assert_called_once()
    # Cache miss path should check the composed key
    mock_redis.get.assert_called_with("daily_activity:2023-01-01:2023-01-31:Running")
    mock_redis.set.assert_called_once()


@pytest.mark.asyncio
async def test_get_daily_summary_cached(client, mock_bq_client, mock_redis):
    cached_data = [
        {
            "activity_date": "2023-01-02",
            "sport": "Cycling",
            "session_count": 2,
            "total_distance_m": 20000.0,
            "total_elapsed_time": 3600.0,
        }
    ]

    mock_redis.get.return_value = json.dumps(cached_data)

    response = await client.get("/api/daily-summary?start_date=2023-01-01&end_date=2023-01-31&sport=Cycling")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["sport"] == "Cycling"
    assert data[0]["session_count"] == 2

    # Should not hit BigQuery when cached
    mock_bq_client.get_daily_activity_summary.assert_not_called()
    mock_redis.get.assert_called_with("daily_activity:2023-01-01:2023-01-31:Cycling")

