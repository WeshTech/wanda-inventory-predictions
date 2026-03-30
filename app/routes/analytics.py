from fastapi import APIRouter, Query, HTTPException

from app.db import get_db_pool
from app.repositories.sales_repo import SalesRepository
from app.services.forecasting.data_prep import ForecastDataPrepService

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



@router.get("/store-sales-prepared")
async def get_store_sales_prepared(
    store_id: str = Query(..., description="Store ID"),
    days: int = Query(180, ge=1, le=365),
):
    repo = SalesRepository()

    records = await repo.get_daily_sales_by_store(store_id=store_id, days=days)

    if not records:
        raise HTTPException(
            status_code=404,
            detail="No sales data found for this store in the selected period."
        )

    df = ForecastDataPrepService.prepare_sales_forecast_data(records)

    try:
        ForecastDataPrepService.validate_minimum_history(df, minimum_days=30)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    return {
        "store_id": store_id,
        "days": days,
        "total_rows": len(df),
        "data": df.to_dict(orient="records")
    }