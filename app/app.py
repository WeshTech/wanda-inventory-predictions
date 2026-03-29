from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.routes import forecast, recommendations, analytics


@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup logic

    yield
    # shutdown


app = FastAPI(
    title="Retail Analytics & Prediction API",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(forecast.router, prefix="/forecast", tags=["Forecast"])
app.include_router(recommendations.router, prefix="/recommendations", tags=["Recommendations"])
app.include_router(analytics.router, prefix="/analytics", tags=["Analytics"])


@app.get("/")
async def root():
    return {
        "message": "Retail Analytics & Prediction API is running"
    }


@app.get("/health")
async def health_check():
    return {
        "status": "ok"
    }