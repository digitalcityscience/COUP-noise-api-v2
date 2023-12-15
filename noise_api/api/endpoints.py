import logging

from celery.result import AsyncResult
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder

import noise_api.tasks as tasks
from noise_api.dependencies import cache, celery_app
from noise_api.models.calculation_input import NoiseCalculationInput, NoiseTask

logger = logging.getLogger(__name__)

router = APIRouter(tags=["jobs"])


@router.post("/processes/traffic-noise/execution")
async def process_job(
    calculation_input: NoiseCalculationInput,
):
    calculation_task = NoiseTask(**calculation_input.dict())
    if result := cache.get(key=calculation_task.celery_key):
        logger.info(
            f"Result fetched from cache with key: {calculation_task.celery_key}"
        )
        return {"job_id": result["job_id"]}

    logger.info(
        f"Result with key: {calculation_task.celery_key} not found in cache. Starting calculation ..."
    )
    result = tasks.compute_task.delay(jsonable_encoder(calculation_task))
    return {"job_id": result.id}


@router.get("/jobs/{job_id}/results")
async def get_job(job_id: str):
    async_result = AsyncResult(job_id, app=celery_app)

    if async_result.successful():
        return {"result": async_result.get()}

    return {
        "job_id": async_result.id,
        "job_state": async_result.state,
    }


@router.get("/jobs/{job_id}/status")
async def get_job_status(job_id: str):
    async_result = AsyncResult(job_id, app=celery_app)
    if async_result.state == "FAILURE":
        return {"status": "FAILURE", "details": {str(async_result.get())}}
    return {"status": async_result.state}
