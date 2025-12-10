from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from fastapi_limiter import FastAPILimiter
import redis.asyncio as redis
from contextlib import asynccontextmanager

from .config import settings
from .routers import sessions, summary, details, daily_activity

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    redis_instance = redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
    await FastAPILimiter.init(redis_instance)
    app.state.redis = redis_instance
    yield
    # Shutdown
    await redis_instance.close()

app = FastAPI(
    title="FIT Data Analysis API",
    description="Backend API for serving fitness data from BigQuery with Redis caching.",
    version="1.0.0",
    lifespan=lifespan
)

# CORS Configuration
origins = settings.CORS_ORIGINS

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include Routers
app.include_router(sessions.router)
app.include_router(summary.router)
app.include_router(details.router)
app.include_router(daily_activity.router)

@app.get("/health")
async def health_check():
    return {"status": "ok"}
