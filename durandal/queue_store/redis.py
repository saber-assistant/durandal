import os
import json
import asyncio
from typing import Callable, Awaitable

from redis.asyncio import Redis
from .base import QueueBackend

# move to conf + add methods to add/pop, that way we can use it in other places and skip http POST
REDIS_LIST_NAME = "intent-tasks"

class RedisQueue(QueueBackend):
    def __init__(self, url: str | None = None) -> None:
        self._r = Redis.from_url(url or os.getenv("REDIS_URL"))
        self._worker_task: asyncio.Task | None = None
        self._shutdown = False

    async def enqueue(self, item):
        await self._r.lpush(REDIS_LIST_NAME, json.dumps(item, default=str))

    async def dequeue(self):
        _, data = await self._r.brpop(REDIS_LIST_NAME)
        return json.loads(data)

    def task_done(self):
        # Redis doesn't need a task_done since it doesn't track in-flight tasks
        pass
    
    async def worker(self, process_func: Callable[[any], Awaitable[None]]) -> None:
        """Start a background worker that processes items from the Redis queue."""
        async def _worker():
            while not self._shutdown:
                try:
                    # Get item from queue (blocks until item available)
                    item = await self.dequeue()
                    
                    # Process the item
                    await process_func(item)
                    
                    # Redis doesn't need explicit task_done
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    # Log error but continue processing
                    print(f"Worker error processing item: {e}")
        
        # Start the worker task
        self._worker_task = asyncio.create_task(_worker())
    
    async def close(self) -> None:
        """Close the Redis connection and stop the worker."""
        self._shutdown = True
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        
        # Close Redis connection
        await self._r.aclose()