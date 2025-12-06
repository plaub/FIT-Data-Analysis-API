import pytest
from unittest.mock import MagicMock
from src.models import SessionSummary, GlobalSummary
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
    mock_redis.get.assert_called_with("sessions_list")     # Should check cache
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
    mock_redis.get.assert_called()

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
