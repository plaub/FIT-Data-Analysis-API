#!/usr/bin/env python3
"""
Cache Clear Script for FIT API

Usage:
    python clear_cache.py                    # Clear all cache
    python clear_cache.py --sessions         # Clear only sessions cache
    python clear_cache.py --summary          # Clear only summary cache
    python clear_cache.py --details          # Clear only details cache
    python clear_cache.py --session-id <id>  # Clear specific session details
"""

import asyncio
import argparse
import redis.asyncio as redis
from src.config import settings


async def clear_all_cache(redis_client):
    """Clear all cache keys"""
    await redis_client.flushdb()
    print("✅ All cache cleared")


async def clear_sessions_cache(redis_client):
    """Clear all sessions pagination cache"""
    pattern = "sessions_list_page_*"
    keys = []
    async for key in redis_client.scan_iter(match=pattern):
        keys.append(key)
    
    if keys:
        await redis_client.delete(*keys)
        print(f"✅ Cleared {len(keys)} sessions cache keys")
    else:
        print("ℹ️  No sessions cache found")


async def clear_summary_cache(redis_client):
    """Clear global summary cache"""
    key = "global_summary"
    result = await redis_client.delete(key)
    if result:
        print(f"✅ Cleared summary cache")
    else:
        print("ℹ️  No summary cache found")


async def clear_details_cache(redis_client, session_id=None):
    """Clear session details cache"""
    if session_id:
        key = f"session_details:{session_id}"
        result = await redis_client.delete(key)
        if result:
            print(f"✅ Cleared details cache for session: {session_id}")
        else:
            print(f"ℹ️  No cache found for session: {session_id}")
    else:
        pattern = "session_details:*"
        keys = []
        async for key in redis_client.scan_iter(match=pattern):
            keys.append(key)
        
        if keys:
            await redis_client.delete(*keys)
            print(f"✅ Cleared {len(keys)} session details cache keys")
        else:
            print("ℹ️  No session details cache found")


async def main():
    parser = argparse.ArgumentParser(description="Clear Redis cache for FIT API")
    parser.add_argument("--all", action="store_true", help="Clear all cache (default)")
    parser.add_argument("--sessions", action="store_true", help="Clear sessions cache")
    parser.add_argument("--summary", action="store_true", help="Clear summary cache")
    parser.add_argument("--details", action="store_true", help="Clear all session details cache")
    parser.add_argument("--session-id", type=str, help="Clear cache for specific session ID")
    
    args = parser.parse_args()
    
    # Connect to Redis with automatic fallback to localhost
    redis_url = settings.REDIS_URL
    redis_client = redis.from_url(redis_url, decode_responses=True)
    
    try:
        # Test connection
        await redis_client.ping()
    except Exception as e:
        # If connection fails and we're not already using localhost, try localhost
        if settings.REDIS_HOST != "localhost":
            print(f"⚠️  Could not connect to {settings.REDIS_HOST}, trying localhost...")
            await redis_client.aclose()
            redis_url = f"redis://localhost:{settings.REDIS_PORT}"
            redis_client = redis.from_url(redis_url, decode_responses=True)
            try:
                await redis_client.ping()
                print(f"✅ Connected to Redis at localhost:{settings.REDIS_PORT}")
            except Exception as e2:
                print(f"❌ Error: Could not connect to Redis at {settings.REDIS_HOST} or localhost")
                print(f"   Make sure Redis is running (e.g., via Docker: docker-compose up -d)")
                await redis_client.aclose()
                return
        else:
            print(f"❌ Error: Could not connect to Redis at {redis_url}")
            print(f"   Make sure Redis is running (e.g., via Docker: docker-compose up -d)")
            await redis_client.aclose()
            return
    
    try:
        # Check if any specific option is set
        specific_option = args.sessions or args.summary or args.details or args.session_id
        
        if args.all or not specific_option:
            await clear_all_cache(redis_client)
        else:
            if args.sessions:
                await clear_sessions_cache(redis_client)
            
            if args.summary:
                await clear_summary_cache(redis_client)
            
            if args.details:
                await clear_details_cache(redis_client)
            
            if args.session_id:
                await clear_details_cache(redis_client, args.session_id)
    
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
