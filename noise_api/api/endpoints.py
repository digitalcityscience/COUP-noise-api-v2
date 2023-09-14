import logging

from celery.result import AsyncResult, GroupResult
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder

import noise_api.tasks as tasks
from noise_api.dependencies import cache, celery_app
from noise_api.models.calculation_input import NoiseCalculationInput, NoiseTask

logger = logging.getLogger(__name__)

router = APIRouter(tags=["tasks"])


@router.post("/task")
async def process_task(
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
    return {"taskId": result.id}


@router.get("/tasks/{task_id}")
async def get_task(task_id: str):
    async_result = AsyncResult(task_id, app=celery_app)

    response = {
        "taskId": async_result.id,
        "taskState": async_result.state,
        "taskSucceeded": async_result.successful(),
        "resultReady": async_result.ready(),
    }

    if async_result.ready():
        response["result"] = async_result.get()

    return response


@router.get("/grouptasks/{group_task_id}")
def get_grouptask(group_task_id: str):
    group_result = GroupResult.restore(group_task_id, app=celery_app)

    # Fields available
    # https://docs.celeryproject.org/en/stable/reference/celery.result.html#celery.result.ResultSet
    return {
        "grouptaskId": group_result.id,
        "tasksCompleted": group_result.completed_count(),
        "tasksTotal": len(group_result.results),
        "grouptaskProcessed": group_result.ready(),
        "grouptaskSucceeded": group_result.successful(),
        "results": [result.get() for result in group_result.results if result.ready()],
    }


@router.get("/tasks/{task_id}/status")
async def get_task_status(task_id: str):
    async_result = AsyncResult(task_id, app=celery_app)
    state = async_result.state
    if state == "FAILURE":
        state = f"FAILURE : {str(async_result.get())}"

    return {"status": state}
