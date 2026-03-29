import asyncpg
from typing import Optional

from app.core.config import settings

db_pool: Optional[asyncpg.Pool] = None


async def connect_to_db():
    global db_pool

    if not settings.DATABASE_URL:
        raise ValueError("DATABASE_URL is not set in the environment variables.")

    db_pool = await asyncpg.create_pool(
        dsn=settings.DATABASE_URL,
        min_size=1,
        max_size=10
    )


async def close_db():
    global db_pool

    if db_pool:
        await db_pool.close()
        db_pool = None


def get_db_pool() -> asyncpg.Pool:
    if db_pool is None:
        raise RuntimeError("Database pool is not initialized.")
    return db_pool