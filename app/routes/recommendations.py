from fastapi import APIRouter, HTTPException, Query

from app.repositories.sales_repo import SalesRepository
from app.services.recommendations.insight_service import RecommendationInsightService

router = APIRouter()


@router.get("/")
async def recommendations_home():
    return {
        "message": "Recommendation routes ready"
    }


@router.get("/regional")
async def get_regional_recommendations(
    county: str | None = Query(None, description="County name"),
    constituency: str | None = Query(None, description="Constituency name"),
    ward: str | None = Query(None, description="Ward name"),
    business_type: str | None = Query(None, description="Business type, e.g. PHARMACY"),
    days: int = Query(3650, ge=1, le=3650),  
    limit: int = Query(100, ge=1, le=100),
):
    if not county and not constituency and not ward:
        raise HTTPException(
            status_code=400,
            detail="At least one regional filter is required: county, constituency, or ward.",
        )

    repo = SalesRepository()


    recommendations = await repo.get_top_selling_products_by_region(
        county=county,
        constituency=constituency,
        ward=ward,
        business_type=business_type,
        days=days,
        limit=limit,
    )


    if not recommendations:
        raise HTTPException(
            status_code=404,
            detail="No regional recommendation data found for the selected filters.",
        )

    return {
        "filters": {
            "county": county,
            "constituency": constituency,
            "ward": ward,
            "business_type": business_type,
            "days": days,
            "limit": limit,
        },
        "total_recommendations": len(recommendations),
        "recommendations": recommendations,
    }

@router.get("/fast-moving/store")
async def get_fast_moving_goods_per_store(
    store_id: str = Query(..., description="Store ID"),
    days: int = Query(3650, ge=1, le=3650),
    limit: int = Query(10, ge=1, le=100),
):
    repo = SalesRepository()
    items = await repo.get_top_selling_products_by_store(
        store_id=store_id,
        days=days,
        limit=limit,
    )

    if not items:
        raise HTTPException(
            status_code=404,
            detail="No fast-moving goods data found for this store.",
        )

    enriched = RecommendationInsightService.build_fast_moving_goods_response(items)

    return {
        "store_id": store_id,
        "days": days,
        "limit": limit,
        "total_items": len(enriched),
        "items": enriched,
    }


@router.get("/weekend-hot-sales")
async def get_weekend_hot_sales(
    store_id: str = Query(..., description="Store ID"),
    days: int = Query(3650, ge=1, le=3650),
    limit: int = Query(10, ge=1, le=100),
):
    repo = SalesRepository()
    items = await repo.get_weekend_hot_sales_by_store(
        store_id=store_id,
        days=days,
        limit=limit,
    )

    if not items:
        raise HTTPException(
            status_code=404,
            detail="No weekend hot sales data found for this store.",
        )

    enriched = RecommendationInsightService.build_weekend_hot_sales_response(items)

    return {
        "store_id": store_id,
        "days": days,
        "limit": limit,
        "total_items": len(enriched),
        "items": enriched,
    }


@router.get("/seasonal-products")
async def get_seasonal_products(
    store_id: str = Query(..., description="Store ID"),
    days: int = Query(3650, ge=30, le=3650),
    limit: int = Query(20, ge=1, le=100),
):
    repo = SalesRepository()
    items = await repo.get_product_sales_by_month_for_store(
        store_id=store_id,
        days=days,
        limit=limit,
    )

    if not items:
        raise HTTPException(
            status_code=404,
            detail="No seasonal product data found for this store.",
        )

    enriched = RecommendationInsightService.build_seasonal_products_response(items)

    return {
        "store_id": store_id,
        "days": days,
        "limit": limit,
        "total_items": len(enriched),
        "items": enriched,
    }


@router.get("/restock-by-business-type")
async def get_restock_data_by_business_type(
    business_type: str = Query(..., description="Business type, e.g. PHARMACY"),
    county: str | None = Query(None, description="County name"),
    constituency: str | None = Query(None, description="Constituency name"),
    ward: str | None = Query(None, description="Ward name"),
    days: int = Query(3650, ge=1, le=3650),
    limit: int = Query(20, ge=1, le=100),
):
    repo = SalesRepository()
    items = await repo.get_restock_candidates_by_business_type(
        business_type=business_type,
        county=county,
        constituency=constituency,
        ward=ward,
        days=days,
        limit=limit,
    )

    if not items:
        raise HTTPException(
            status_code=404,
            detail="No restock recommendation data found for the selected business type and region.",
        )

    enriched = RecommendationInsightService.build_restock_response(items)

    return {
        "filters": {
            "business_type": business_type,
            "county": county,
            "constituency": constituency,
            "ward": ward,
            "days": days,
            "limit": limit,
        },
        "total_items": len(enriched),
        "items": enriched,
    }