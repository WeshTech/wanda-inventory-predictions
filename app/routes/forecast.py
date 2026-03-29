from fastapi import APIRouter

router = APIRouter()


@router.get("/")
async def forecast_home():
    return {
        "message": "Forecast routes ready"
    }