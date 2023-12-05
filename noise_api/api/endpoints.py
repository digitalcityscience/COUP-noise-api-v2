import logging

from celery.result import AsyncResult
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder

import noise_api.tasks as tasks
from noise_api.dependencies import cache, celery_app
from noise_api.models.calculation_input import NoiseCalculationInput, NoiseTask

logger = logging.getLogger(__name__)

router = APIRouter(tags=["tasks"])


@router.post("/processes/noise/execution")
async def execute_noise(
    calculation_input: NoiseCalculationInput,
):
    calculation_task = NoiseTask(**calculation_input.dict())
    if result := cache.get(key=calculation_task.celery_key):
        logger.info(
            f"Result fetched from cache with key: {calculation_task.celery_key}"
        )
        return result

    logger.info(
        f"Result with key: {calculation_task.celery_key} not found in cache. Starting calculation ..."
    )
    result = tasks.compute_task.delay(jsonable_encoder(calculation_task))
    return {"task_id": result.id}


@router.get("/jobs/{job_id}/results")
async def get_task(job_id: str):
    async_result = AsyncResult(job_id, app=celery_app)

    response = {
        "task_id": async_result.id,
        "task_state": async_result.state,
        "task_succeeded": async_result.successful(),
        "result_ready": async_result.ready(),
    }

    if async_result.ready():
        response["result"] = async_result.get()

    return response


@router.get("/jobs/{job_id}")
async def get_task_status(job_id: str):
    async_result = AsyncResult(job_id, app=celery_app)
    state = async_result.state
    if state == "FAILURE":
        state = f"FAILURE : {str(async_result.get())}"

    return {"status": state}
