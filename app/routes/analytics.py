from fastapi import APIRouter

router = APIRouter()


@router.get("/")
async def analytics_home():
    return {
        "message": "Analytics routes ready"
    }