import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from noise_api.api.endpoints import router as tasks_router
from noise_api.config import settings
from noise_api.logs import setup_logging

setup_logging()

app = FastAPI(
    title=settings.title,
    descriprition=settings.description,
    version=settings.version,
    redoc_url="/noise/redoc",
    docs_url="/noise/docs",
    openapi_url="/noise/openapi.json",
)

# TODO replace origins
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get(
    "/noise/openapi.json",
)
async def openapi():
    return app.openapi()


@app.get("/noise/health_check", tags=["ROOT"])
async def health_check():
    return "ok"


app.include_router(tasks_router, prefix="/noise")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=80)
