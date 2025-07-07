# asyncpg-lock

Run long-running work exclusively using PostgreSQL advisory locks.

## Usage

```python
import asyncio
import asyncpg
import asyncpg_lock


async def worker_func() -> None:
    # something very long-running
    while True:
        await asyncio.sleep(100500)


async def main():
    guard = asyncpg_lock.AdvisoryLockGuard(
        connect=lambda: asyncpg.connect("postgresql://localhost/db")
    )
    # worker_func will not be executed concurrently
    key = 100500
    tasks = [
        asyncio.create_task(guard.run(key, worker_func)),
        asyncio.create_task(guard.run(key, worker_func)),
        asyncio.create_task(guard.run(key, worker_func)),
        asyncio.create_task(guard.run(key, worker_func))
    ]
    await asyncio.gather(*tasks)


asyncio.run(main())
```
