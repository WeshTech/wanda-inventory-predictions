from fastapi import APIRouter, Query

from app.db import get_db_pool
from app.repositories.sales_repo import SalesRepository

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


@router.get("/store-sales")
async def get_store_sales(
    store_id: str = Query(..., description="Store ID"),
    days: int = Query(180, ge=1, le=365),
):
    repo = SalesRepository()
    data = await repo.get_daily_sales_by_store(store_id=store_id, days=days)

    return {
        "store_id": store_id,
        "days": days,
        "records": data
    }