from fastapi import APIRouter

router = APIRouter()


@router.get("/")
async def recommendations_home():
    return {
        "message": "Recommendation routes ready"
    }