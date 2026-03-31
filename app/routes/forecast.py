from fastapi import APIRouter, HTTPException, Query

from app.repositories.sales_repo import SalesRepository
from app.services.forecasting.data_prep import ForecastDataPrepService
from app.services.forecasting.prophet_service import ProphetForecastService

router = APIRouter()


@router.get("/")
async def forecast_home():
    return {
        "message": "Forecast routes ready"
    }


@router.get("/store-sales")
async def forecast_store_sales(
    store_id: str = Query(..., description="Store ID"),
    history_days: int = Query(180, ge=30, le=365),
    forecast_days: int = Query(30, ge=1, le=30),
):
    repo = SalesRepository()
    records = await repo.get_daily_sales_by_store(
        store_id=store_id,
        days=history_days,
    )

    if not records:
        raise HTTPException(
            status_code=404,
            detail="No sales data found for this store in the selected period.",
        )

    df = ForecastDataPrepService.prepare_sales_forecast_data(records)

    try:
        ForecastDataPrepService.validate_minimum_history(df, minimum_days=30)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    try:
        prophet_service = ProphetForecastService()
        forecast = prophet_service.forecast_only_future(
            df=df,
            periods=forecast_days,
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Forecast generation failed: {str(e)}",
        )

    return {
        "store_id": store_id,
        "history_days": history_days,
        "forecast_days": forecast_days,
        "historical_points": len(df),
        "forecast": forecast,
    }


@router.get("/product-sales")
async def forecast_product_sales(
    store_id: str = Query(..., description="Store ID"),
    store_product_id: str = Query(..., description="Store Product ID"),
    history_days: int = Query(180, ge=30, le=365),
    forecast_days: int = Query(30, ge=1, le=30),
):
    repo = SalesRepository()
    records = await repo.get_daily_product_sales_for_store(
        store_id=store_id,
        store_product_id=store_product_id,
        days=history_days,
    )

    if not records:
        raise HTTPException(
            status_code=404,
            detail="No product sales data found for this store in the selected period.",
        )

    df = ForecastDataPrepService.prepare_product_quantity_forecast_data(records)

    try:
        ForecastDataPrepService.validate_minimum_history(df, minimum_days=30)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    try:
        prophet_service = ProphetForecastService()
        forecast = prophet_service.forecast_only_future(
            df=df,
            periods=forecast_days,
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Product forecast generation failed: {str(e)}",
        )

    return {
        "store_id": store_id,
        "store_product_id": store_product_id,
        "history_days": history_days,
        "forecast_days": forecast_days,
        "historical_points": len(df),
        "forecast": forecast,
    }