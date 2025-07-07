import asyncio
import logging
from typing import Any, Awaitable, Callable, Coroutine

import asyncpg

from .utils import run_forever, run_periodically

logger = logging.getLogger(__package__)

ConnectFunc = Callable[[], Coroutine[Any, Any, asyncpg.Connection]]


def connect_func(*args: Any, **kwargs: Any) -> ConnectFunc:
    async def _connect() -> asyncpg.Connection:
        return await asyncpg.connect(*args, **kwargs)

    return _connect


class LockManager:
    __slots__ = (
        "__connect",
        "__reconnect_delay",
        "__after_acquire_delay",
        "__reacquire_delay",
    )

    def __init__(
        self,
        *,
        connect: ConnectFunc = asyncpg.connect,
        reconnect_delay: float = 5,
        reacquire_delay: float = 5,
        after_acquire_delay: float = 5,
    ) -> None:
        if reconnect_delay < 0:
            raise ValueError("reconnect_delay must be non-negative")
        if reacquire_delay <= 0:
            raise ValueError("reacquire_delay must be positive")
        if after_acquire_delay <= 0:
            raise ValueError("after_acquire_delay must be positive")

        self.__reconnect_delay = reconnect_delay
        self.__after_acquire_delay = after_acquire_delay
        self.__reacquire_delay = reacquire_delay
        self.__connect = connect

    async def guard(
        self,
        func: Callable[[], Coroutine],
        *,
        lock_ns: int,
        lock_key: int,
    ) -> None:
        await self.__ensure_is_connected(
            lambda connection: self.__ensure_lock_acquired(
                connection,
                lambda: run_forever(func),
                lock_ns=lock_ns,
                lock_key=lock_key,
            )
        )

    async def __ensure_is_connected(self, func: Callable[[asyncpg.Connection], Awaitable[None]]) -> None:
        while True:
            try:
                connection = await self.__connect()
                logger.info("Connection is established")
                try:
                    await func(connection)
                finally:
                    await asyncio.shield(connection.close())
                    logger.info("Connection is closed")
            except Exception:
                logger.exception("Failed to connect or to execute a function")

            await asyncio.sleep(self.__reconnect_delay)

    async def __ensure_lock_acquired(
        self,
        connection: asyncpg.Connection,
        func: Callable[[], Coroutine],
        *,
        lock_ns: int,
        lock_key: int,
    ) -> None:
        while True:
            if connection.is_closed():
                logger.debug("Connection is blocked")
                return

            try:
                acquired = await connection.fetchval("SELECT pg_try_advisory_lock($1, $2)", lock_ns, lock_key)
            except Exception:
                logger.debug("Exception during acquiring an advisory lock", exc_info=True)
                return

            if acquired:
                break

            await asyncio.sleep(self.__reacquire_delay)

        logger.info(
            "Lock (%s, %s) is acquired, waiting %s s grace period",
            lock_ns,
            lock_key,
            self.__after_acquire_delay,
        )
        await asyncio.sleep(self.__after_acquire_delay)
        logger.info("Lock (%s, %s) is acquired, running", lock_ns, lock_key)

        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    run_periodically(
                        lambda t: connection.execute("SELECT 1", timeout=t),
                        self.__after_acquire_delay / 3,
                    )
                )
                tg.create_task(func())
        except Exception:
            logger.debug("Exception during running a function or monitoring a connection", exc_info=True)

        logger.warning("Lock (%s, %s) is lost", lock_ns, lock_key)
