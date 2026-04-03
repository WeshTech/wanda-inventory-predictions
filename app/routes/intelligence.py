from fastapi import APIRouter, HTTPException, Query

from app.repositories.intelligence_repo import IntelligenceRepository
from app.services.intelligence.intelligence_service import IntelligenceService

router = APIRouter()


@router.get("/")
async def intelligence_home():
    return {
        "message": "Intelligence routes ready"
    }


@router.get("/store")
async def get_store_intelligence(
    store_id: str = Query(..., description="Store ID"),
    county: str = Query("Muranga", description="County"),
    constituency: str = Query("Kiharu", description="Constituency"),
    ward: str = Query("Township", description="Ward"),
):
    repo = IntelligenceRepository()
    service = IntelligenceService(repo)

    try:
        result = await service.build_store_intelligence(
            store_id=store_id,
            county=county,
            constituency=constituency,
            ward=ward,
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate intelligence: {str(e)}"
        )