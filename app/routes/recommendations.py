from fastapi import APIRouter, HTTPException, Query

from app.repositories.sales_repo import SalesRepository

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
    limit: int = Query(100, ge=1, le=101),
):
    if not county and not constituency and not ward:
        raise HTTPException(
            status_code=400,
            detail="At least one regional filter is required: county, constituency, or ward.",
        )

    repo = SalesRepository()

    print(county, constituency, ward, business_type, days, limit)

    recommendations = await repo.get_top_selling_products_by_region(
        county=county,
        constituency=constituency,
        ward=ward,
        business_type=business_type,
        days=days,
        limit=limit,
    )

    print(recommendations)

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