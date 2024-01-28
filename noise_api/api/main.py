import uvicorn
from fastapi import FastAPI

from noise_api.api.endpoints import router as tasks_router
from noise_api.config import settings
from noise_api.logs import setup_logging

setup_logging()

API_PREFIX = "/noise"

app = FastAPI(
    title=settings.title,
    descriprition=settings.description,
    version=settings.version,
    redoc_url=f"{API_PREFIX}/redoc",
    docs_url=f"{API_PREFIX}/docs",
    openapi_url=f"{API_PREFIX}/openapi.json",
)


@app.get(f"{API_PREFIX}/health_check", tags=["ROOT"])
async def health_check():
    return "ok"


app.include_router(tasks_router, prefix=API_PREFIX)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=80)
