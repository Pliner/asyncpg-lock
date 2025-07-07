import asyncio
import logging
import time
from typing import Callable, Coroutine

logger = logging.getLogger(__package__)


async def run_periodically(
    func: Callable[[float], Coroutine],
    interval: float,
) -> None:
    while True:
        func_started_at = time.monotonic()
        await asyncio.wait_for(func(interval), timeout=interval)
        func_finished_at = time.monotonic()
        elapsed = func_finished_at - func_started_at
        if elapsed > interval * 2:
            raise asyncio.TimeoutError()
        if elapsed < interval:
            await asyncio.sleep(interval - elapsed)


async def run_forever(func: Callable[[], Coroutine]) -> None:
    while True:
        try:
            await func()
        except Exception:
            logger.exception("An error occurred while executing func")
