import asyncio
import logging
from contextlib import asynccontextmanager
import uuid
from typing import Literal

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

from .queue_store import get_queue, QueueBackend
from .conf import conf


# --------------------------------------------------------------------------- #
logger = logging.getLogger(conf.APP_NAME)
logging.basicConfig(level=conf.LOG_LEVEL, format=conf.LOG_FORMAT)

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def validate_api_key(key: str | None = Depends(api_key_header)) -> None:
    """FastAPI dependency that aborts if the key is bad/missing."""
    if key is None or key != conf.API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )


task_queue: QueueBackend


# --------------------------------------------------------------------------- #
# Task schema
# --------------------------------------------------------------------------- #
class TaskIn(BaseModel):
    task_id: uuid.UUID = Field()
    job: Literal["classify"]
    content: str
    is_partial: bool = False
    job_budget: int = 10
    callback_url: str | None = None
    priority_order: Literal["ascending", "descending"] = "ascending"


# --------------------------------------------------------------------------- #
# FastAPI
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Started lifespan context.")

    global task_queue, result_store
    
    # Initialize queue based on settings
    queue_settings = conf.QUEUE_SETTINGS.get(conf.QUEUE_TYPE, {})
    task_queue = get_queue(conf.QUEUE_TYPE, **queue_settings)

    asyncio.create_task(task_queue.worker(process_task))

    logger.info("Started queue worker.")

    try:
        yield
    finally:
        await task_queue.close()
        logger.info("Stopped queue worker.")


app = FastAPI(title=conf.APP_NAME, lifespan=lifespan)


@app.post("/queue/", dependencies=[Depends(validate_api_key)], status_code=202)
async def enqueue_task(task) -> dict[str, str]:
    pass


async def process_task(task) -> None:
    pass