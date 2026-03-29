from fastapi import APIRouter
from app.db import get_db_pool

router = APIRouter()


@router.get("/")
async def analytics_home():
    return {
        "message": "Analytics routes ready"
    }


@router.get("/db-check")
async def db_check():
    pool = get_db_pool()

    async with pool.acquire() as connection:
        result = await connection.fetchval("SELECT 1;")

    return {
        "database": "connected",
        "result": result
    }