from .memory import MemoryQueue
from .redis import RedisQueue
from .base import QueueBackend

QUEUE_MAPPING = {
    "memory": MemoryQueue,
    "redis": RedisQueue,
}

def get_queue(queue_type: str, **kwargs) -> QueueBackend:
    """
    Get a queue instance based on the queue type.
    
    Args:
        queue_type: Type of queue ("memory" or "redis")
        **kwargs: Additional arguments passed to the queue constructor
    
    Returns:
        QueueBackend instance
    """
    queue_class = QUEUE_MAPPING.get(queue_type)
    if not queue_class:
        raise ValueError(f"Unknown queue type: {queue_type}")
    return queue_class(**kwargs)

__all__ = ["QueueBackend", "MemoryQueue", "RedisQueue", "get_queue"]
