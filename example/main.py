import asyncio
import random
import logging

import asyncpg
import asyncpg_lock

logger = logging.getLogger(__name__)


async def process(id: int) -> None:
    while True:
        logger.info("Processing %s...", id)
        await asyncio.sleep(5)
        logger.info("Done processing %s", id)


async def main():
    lock_manager = asyncpg_lock.LockManager(connect=lambda: asyncpg.connect(database="postgres"))
    lock_key = (42, 42)
    process_one_id = random.randint(0, 100500)
    process_one = asyncio.create_task(
        lock_manager.guard(
            lambda: process(process_one_id),
            key=lock_key,
        )
    )
    process_two_id = random.randint(0, 100500)
    process_two = asyncio.create_task(
        lock_manager.guard(
            lambda: process(process_two_id),
            key=lock_key,
        )
    )
    await asyncio.gather(process_one, process_two)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )

    asyncio.run(main())
