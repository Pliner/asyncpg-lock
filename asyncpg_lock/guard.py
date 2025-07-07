import asyncio
import logging
import time
from typing import Any, Callable, Coroutine

import asyncpg

logger = logging.getLogger(__package__)

ConnectFunc = Callable[[], Coroutine[Any, Any, asyncpg.Connection]]


def connect_func(*args: Any, **kwargs: Any) -> ConnectFunc:
    async def _connect() -> asyncpg.Connection:
        return await asyncpg.connect(*args, **kwargs)

    return _connect


class AdvisoryLockGuard:
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

    async def run(
        self,
        key: int | tuple[int, int],
        func: Callable[[], Coroutine],
    ) -> None:
        if isinstance(key, int):
            if not (-(2**63) <= key < 2**63):
                raise ValueError("key must be a signed 64-bit integer")
        else:
            if not (-(2**31) <= key[0] < 2**31) or not (-(2**31) <= key[1] < 2**31):
                raise ValueError("key must be a tuple of two signed 32-bit integers")

        await self.__ensure_connection_established(
            lambda connection: self.__ensure_lock_acquired(connection, key, func)
        )

    async def __ensure_connection_established(self, func: Callable[[asyncpg.Connection], Coroutine]) -> None:
        failed_attempts = 0
        while True:
            try:
                connection = await self.__connect()
                failed_attempts = 0
                try:
                    await func(connection)
                finally:
                    await asyncio.shield(connection.close())
            except Exception:
                if failed_attempts > 0:
                    logger.exception("Connection closed or not established")
                failed_attempts += 1

            await asyncio.sleep(self.__reconnect_delay)

    async def __ensure_lock_acquired(
        self,
        connection: asyncpg.Connection,
        key: int | tuple[int, int],
        func: Callable[[], Coroutine],
    ) -> None:
        logger.info("Acquiring lock %s", key)
        await self.__acquire_lock(connection, key)
        logger.info("Lock %s acquired, waiting for %s s", key, self.__after_acquire_delay)
        await asyncio.sleep(self.__after_acquire_delay)
        logger.info("Lock %s acquired, running", key)
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self.__ensure_connection_alive(connection, key))
                tg.create_task(self.__run_forever(func))
        finally:
            logger.info("Lock %s might be lost", key)

    async def __acquire_lock(self, connection: asyncpg.Connection, key: int | tuple[int, int]) -> None:
        while True:
            try:
                if isinstance(key, int):
                    acquired = await connection.fetchval("SELECT pg_try_advisory_lock($1)", key)
                else:
                    acquired = await connection.fetchval("SELECT pg_try_advisory_lock($1, $2)", key[0], key[1])
            except Exception:
                raise Exception(f"Lock {key} not acquired")
            if acquired:
                return
            await asyncio.sleep(self.__reacquire_delay)

    async def __ensure_connection_alive(self, connection: asyncpg.Connection, key: int | tuple[int, int]) -> None:
        per_attempt_keep_alive_budget = self.__after_acquire_delay / 3
        while True:
            started_at = time.monotonic()
            if connection.is_closed():
                raise Exception(f"Lock {key} might be lost: connection is closed")
            try:
                await connection.execute("SELECT 1", timeout=per_attempt_keep_alive_budget)
            except asyncio.TimeoutError:
                raise Exception(f"Lock {key} might be lost: keep-alive query timed out")
            except Exception:
                raise Exception(f"Lock {key} might be lost: keep-alive query failed")

            finished_at = time.monotonic()
            elapsed = finished_at - started_at
            if elapsed > per_attempt_keep_alive_budget * 2:
                raise Exception(f"Lock {key} might be lost: keep-alive query took too long")
            if elapsed < per_attempt_keep_alive_budget:
                await asyncio.sleep(per_attempt_keep_alive_budget - elapsed)

    @staticmethod
    async def __run_forever(func: Callable[[], Coroutine]) -> None:
        while True:
            try:
                await func()
            except Exception:
                logger.exception("Unexpected exception")
