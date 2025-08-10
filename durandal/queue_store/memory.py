import asyncio
from typing import Any, Callable, Awaitable

from .base import QueueBackend

class MemoryQueue(QueueBackend):
    def __init__(self) -> None:
        self._q: asyncio.Queue[Any] = asyncio.Queue()
        self._worker_task: asyncio.Task | None = None
        self._shutdown = False

    async def enqueue(self, item): 
        await self._q.put(item)
        
    async def dequeue(self): 
        return await self._q.get()
        
    def task_done(self): 
        self._q.task_done()
    
    async def worker(self, process_func: Callable[[Any], Awaitable[None]]) -> None:
        """Start a background worker that processes items from the queue."""
        async def _worker():
            while not self._shutdown:
                try:
                    # Get item from queue
                    item = await self.dequeue()
                    
                    # Process the item
                    await process_func(item)
                    
                    # Mark task as done
                    self.task_done()
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    # Log error but continue processing
                    print(f"Worker error processing item: {e}")
                    # Still mark task as done to prevent queue from hanging
                    self.task_done()
        
        # Start the worker task
        self._worker_task = asyncio.create_task(_worker())
    
    async def close(self) -> None:
        """Close the queue and stop the worker."""
        self._shutdown = True
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass