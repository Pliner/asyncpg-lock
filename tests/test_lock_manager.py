import asyncio
import contextlib
import dataclasses
import random
import time
from typing import Awaitable, Callable

import asyncpg
import pytest
import pytest_pg

import asyncpg_lock

from .conftest import TcpProxy

NOOP_DELAY = 0.05
RECONNECT_DELAY = 0.1
LOCK_ACQUIRE_GRACE_PERIOD = 0.75
LOCK_ACQUIRE_RETRY_INTERVAL = 0.5
PER_ATTEMPT_DELAY = 0.1

LOCK_KEY = random.randint(0, 2**63 - 1)
NON_CONFLICTING_LOCK_KEY = random.randint(-(2**63), -1)


async def cancel_and_wait(future: asyncio.Future[None]) -> None:
    future.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await future


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class Execution:
    started_at: float
    ended_at: float | None = None

    @property
    def in_progress(self) -> bool:
        return self.ended_at is None

    @property
    def duration(self) -> float:
        if self.ended_at is None:
            raise ValueError("Execution is still in progress")
        return self.ended_at - self.started_at


class ExecutionTracker:
    def __init__(self, min_completed_executions: int = 1, max_completed_executions: int | None = None) -> None:
        self.executions: list[Execution] = []
        self.min_completed_executions = min_completed_executions
        self.max_completed_executions = (
            min_completed_executions if max_completed_executions is None else max_completed_executions
        )
        assert self.min_completed_executions <= self.max_completed_executions
        self.min_completed_executions_event = asyncio.Event()
        self.max_completed_executions_event = asyncio.Event()
        if self.min_completed_executions == 0:
            self.min_completed_executions_event.set()
        if self.max_completed_executions == 0:
            self.max_completed_executions_event.set()
        self.lock = asyncio.Lock()
        self.overlaps = 0

    async def wait_min_completed(self) -> None:
        base_timeout = LOCK_ACQUIRE_GRACE_PERIOD + PER_ATTEMPT_DELAY * self.min_completed_executions
        adjusted_timeout = NOOP_DELAY + base_timeout * 1.10
        async with asyncio.timeout(adjusted_timeout):
            await self.min_completed_executions_event.wait()

    async def wait_max_completed(self) -> None:
        base_timeout = LOCK_ACQUIRE_GRACE_PERIOD + PER_ATTEMPT_DELAY * self.max_completed_executions
        adjusted_timeout = NOOP_DELAY + base_timeout * 1.10
        async with asyncio.timeout(adjusted_timeout):
            await self.max_completed_executions_event.wait()

    async def __call__(self) -> None:
        if self.lock.locked():
            self.overlaps += 1
            raise ValueError("Overlap is detected")

        async with self.lock:
            while True:
                self.executions.append(Execution(started_at=time.monotonic()))
                execution_idx = len(self.executions) - 1
                try:
                    await asyncio.sleep(PER_ATTEMPT_DELAY)
                finally:
                    self.executions[execution_idx] = dataclasses.replace(
                        self.executions[execution_idx], ended_at=time.monotonic()
                    )
                    completed_executions_count = self.__count_completed_executions(self.executions)
                    if completed_executions_count >= self.min_completed_executions:
                        self.min_completed_executions_event.set()
                    if completed_executions_count >= self.max_completed_executions:
                        self.max_completed_executions_event.set()

    def has_overlap_with(self, other: "ExecutionTracker") -> bool:
        return self.__has_overlapping_executions(self.executions + other.executions)

    @classmethod
    def __count_completed_executions(cls, executions: list[Execution]) -> int:
        return sum(1 for execution in executions if not execution.in_progress)

    @classmethod
    def __has_overlapping_executions(cls, executions: list[Execution]) -> bool:
        for i in range(len(executions)):
            for j in range(i + 1, len(executions)):
                if executions[i].in_progress or executions[j].in_progress:
                    continue
                if not (
                    executions[i].ended_at <= executions[j].started_at  # type: ignore[operator]
                    or executions[j].ended_at <= executions[i].started_at  # type: ignore[operator]
                ):
                    return True
        return False


@pytest.fixture
async def proxy(
    pg_14: pytest_pg.PG, unused_port: Callable[[], int], tcp_proxy: Callable[[int, int], Awaitable[TcpProxy]]
) -> TcpProxy:
    return await tcp_proxy(unused_port(), pg_14.port)


class PgConnector:
    def __init__(self, connect_func: asyncpg_lock.ConnectFunc) -> None:
        self.total_open_connections = 0
        self.connect_func = connect_func

    async def __call__(self) -> asyncpg.Connection:
        connection = await self.connect_func()
        self.total_open_connections += 1
        return connection


@pytest.fixture
def connector(proxy: TcpProxy, pg_14: pytest_pg.PG) -> PgConnector:
    return PgConnector(
        asyncpg_lock.connect_func(
            host=pg_14.host,
            port=proxy.src_port,
            user=pg_14.user,
            database=pg_14.database,
        )
    )


@pytest.fixture
def lock_manager(connector: PgConnector) -> asyncpg_lock.LockManager:
    return asyncpg_lock.LockManager(
        connect=connector,
        reconnect_delay=RECONNECT_DELAY,
        after_acquire_delay=LOCK_ACQUIRE_GRACE_PERIOD,
        reacquire_delay=LOCK_ACQUIRE_RETRY_INTERVAL,
    )


@pytest.mark.parametrize(
    "key",
    [
        -(2**63) - 1,
        2**63,
        (-(2**31) - 1, 0),
        (2**31, 0),
        (0, -(2**31) - 1),
        (0, 2**31),
    ],
)
async def test_invalid_lock_key(lock_manager: asyncpg_lock.LockManager, key) -> None:
    with pytest.raises(ValueError, match="key must be"):
        await lock_manager.guard(lambda: asyncio.sleep(-1), key=key)


async def test_acquired_lock_holds(lock_manager: asyncpg_lock.LockManager, connector: PgConnector) -> None:
    tracker = ExecutionTracker(min_completed_executions=int(LOCK_ACQUIRE_GRACE_PERIOD // PER_ATTEMPT_DELAY) * 2)
    task = asyncio.create_task(lock_manager.guard(tracker, key=LOCK_KEY))
    try:
        await tracker.wait_min_completed()
    finally:
        await cancel_and_wait(task)

    assert connector.total_open_connections == 1
    assert not tracker.overlaps


async def test_reacquire_lock_after_completion(lock_manager: asyncpg_lock.LockManager, connector: PgConnector) -> None:
    tracker = ExecutionTracker(min_completed_executions=8, max_completed_executions=16)
    task = asyncio.create_task(lock_manager.guard(tracker, key=LOCK_KEY))
    try:
        await tracker.wait_min_completed()
    finally:
        await cancel_and_wait(task)

    task = asyncio.create_task(lock_manager.guard(tracker, key=LOCK_KEY))
    try:
        await tracker.wait_max_completed()
    finally:
        await cancel_and_wait(task)

    assert connector.total_open_connections == 2
    assert not tracker.overlaps


async def test_reacquire_lock_after_disruption(
    lock_manager: asyncpg_lock.LockManager, connector: PgConnector, proxy: TcpProxy
) -> None:
    tracker = ExecutionTracker(min_completed_executions=8, max_completed_executions=16)
    task = asyncio.create_task(lock_manager.guard(tracker, key=LOCK_KEY))
    try:
        await tracker.wait_min_completed()
        await proxy.drop_connections()
        await tracker.wait_max_completed()
    finally:
        await cancel_and_wait(task)

    assert connector.total_open_connections == 2
    assert not tracker.overlaps


async def test_no_overlapping_execution_for_same_keys(
    lock_manager: asyncpg_lock.LockManager, connector: PgConnector
) -> None:
    tracker = ExecutionTracker(min_completed_executions=16)

    task_0 = asyncio.create_task(lock_manager.guard(tracker, key=LOCK_KEY))
    task_1 = asyncio.create_task(lock_manager.guard(tracker, key=LOCK_KEY))

    try:
        await tracker.wait_min_completed()
    finally:
        await cancel_and_wait(task_0)
        await cancel_and_wait(task_1)

    assert connector.total_open_connections == 2
    assert not tracker.overlaps


async def test_overlapping_execution_for_different_keys(
    lock_manager: asyncpg_lock.LockManager, connector: PgConnector
) -> None:
    tracker_1 = ExecutionTracker(min_completed_executions=int(LOCK_ACQUIRE_GRACE_PERIOD // PER_ATTEMPT_DELAY) * 2)
    tracker_2 = ExecutionTracker(min_completed_executions=int(LOCK_ACQUIRE_GRACE_PERIOD // PER_ATTEMPT_DELAY) * 2)

    task_1 = asyncio.create_task(lock_manager.guard(tracker_1, key=LOCK_KEY))
    task_2 = asyncio.create_task(lock_manager.guard(tracker_2, key=NON_CONFLICTING_LOCK_KEY))
    try:
        await asyncio.gather(tracker_1.wait_min_completed(), tracker_2.wait_min_completed())
        assert tracker_1.has_overlap_with(tracker_2)
    finally:
        await cancel_and_wait(task_1)
        await cancel_and_wait(task_2)

    assert connector.total_open_connections == 2
    assert not tracker_1.overlaps
    assert not tracker_2.overlaps


async def test_no_overlapping_execution_for_same_keys_dropped_connection(
    lock_manager: asyncpg_lock.LockManager,
    proxy: TcpProxy,
) -> None:
    tracker = ExecutionTracker(min_completed_executions=8, max_completed_executions=16)

    tasks = []
    max_tasks_count = 4
    for _ in range(max_tasks_count):
        task = asyncio.create_task(lock_manager.guard(tracker, key=LOCK_KEY))
        tasks.append(task)

    try:
        await tracker.wait_min_completed()
        await proxy.drop_connections()
        await tracker.wait_max_completed()
    finally:
        await asyncio.gather(*[cancel_and_wait(task) for task in tasks])

    assert not tracker.overlaps
