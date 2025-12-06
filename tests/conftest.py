import pytest
from httpx import AsyncClient, ASGITransport
from src.main import app
from src.dependencies import get_redis, get_bq_client
from unittest.mock import AsyncMock, MagicMock
from src.models import SessionSummary, GlobalSummary
from datetime import datetime

# Fixture for the FastAPI client
@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac

# Mock Redis
@pytest.fixture
def mock_redis():
    mock = AsyncMock()
    # Mocking get to return None by default (cache miss)
    mock.get.return_value = None
    # Mocking evalsha for Rate Limiter
    mock.evalsha.return_value = 0
    return mock

from fastapi_limiter import FastAPILimiter

# Mock BigQuery Client
@pytest.fixture
def mock_bq_client():
    mock = MagicMock()
    return mock

@pytest.fixture(autouse=True)
async def init_limiter(mock_redis):
    await FastAPILimiter.init(mock_redis)
    yield
    await FastAPILimiter.close()

# Override dependencies
@pytest.fixture(autouse=True)
def override_dependencies(mock_redis, mock_bq_client):
    app.dependency_overrides[get_redis] = lambda: mock_redis
    app.dependency_overrides[get_bq_client] = lambda: mock_bq_client
    yield
    app.dependency_overrides = {}
