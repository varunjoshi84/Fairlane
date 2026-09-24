import logging
from redis.asyncio import Redis
from fairlane.config import settings

logger = logging.getLogger(__name__)

# Create the async Redis client
redis_client = Redis.from_url(settings.redis_url, decode_responses=True)

async def check_redis_health() -> bool:
    """Execute a simple PING query to verify Redis connectivity."""
    try:
        return await redis_client.ping()
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        return False

async def push_task_to_stream(task_id: str) -> None:
    """Push a task ID to the Redis Stream."""
    try:
        await redis_client.xadd(settings.redis_stream_name, {"task_id": task_id})
    except Exception as e:
        logger.error(f"Failed to push task {task_id} to stream: {e}")
        raise
