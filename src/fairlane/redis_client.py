"""Redis client helpers and stream utilities."""

import logging

from redis.asyncio import Redis

from fairlane.config import settings

logger = logging.getLogger(__name__)


def get_redis_client() -> Redis:
    """Create a new async Redis client."""
    return Redis.from_url(settings.redis_url, decode_responses=True)


async def check_redis_health() -> bool:
    """Execute a simple PING query to verify Redis connectivity."""
    client = get_redis_client()
    try:
        res = await client.ping()
        return bool(res)
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        return False
    finally:
        await client.aclose()


async def push_task_to_stream(task_id: str) -> None:
    """Push a task ID to the Redis Stream."""
    # TODO: Implement multi-stream priority routing
    # TODO: Implement rate limiting before stream push
    client = get_redis_client()
    try:
        await client.xadd(settings.redis_stream_name, {"task_id": task_id})
    except Exception as e:
        logger.error(f"Failed to push task {task_id} to stream: {e}")
        raise
    finally:
        await client.aclose()
