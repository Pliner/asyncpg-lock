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
    lock_manager = asyncpg_lock.LockManager(
        connect=lambda: asyncpg.connect("postgresql://localhost/db")
    )
    # worker_func will not be executed concurrently
    guard_tasks = [
        asyncio.create_task(lock_manager.guard(worker_func, lock_ns=100, lock_key=1)),
        asyncio.create_task(lock_manager.guard(worker_func, lock_ns=100, lock_key=1)),
        asyncio.create_task(lock_manager.guard(worker_func, lock_ns=100, lock_key=1)),
        asyncio.create_task(lock_manager.guard(worker_func, lock_ns=100, lock_key=1))
    ]
    await asyncio.gather(*guard_tasks)

asyncio.run(main())
```
