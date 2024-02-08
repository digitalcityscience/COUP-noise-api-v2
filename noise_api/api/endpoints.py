import logging
from typing import Annotated

from celery.result import AsyncResult
from fastapi import APIRouter, Body
from fastapi.encoders import jsonable_encoder
from fastapi.openapi.utils import get_openapi

import noise_api.tasks as tasks
from noise_api.api.ogc_docs import get_processes
from noise_api.dependencies import cache, celery_app
from noise_api.models.calculation_input import (
    BUILDINGS,
    ROADS,
    NoiseCalculationInput,
    NoiseTask,
)
from noise_api.utils import load_json_file

logger = logging.getLogger(__name__)

router = APIRouter(tags=["jobs"])


def generate_openapi_json():
    return get_openapi(title=os.environ["APP_TITLE"], version="1.0.0", routes=router.routes, openapi_version="3.0.0")


@router.get("/processes/{process_id}")
@router.get("/processes")
async def get_processes_json(process_id: str = None) -> dict:
    processes = get_processes(generate_openapi_json())

    if process_id:
        for process in processes["processes"]:
            print(process)
            if process["id"] == process_id:
                return process

    return processes



@router.post(
    path="/processes/traffic-noise/execution",
    tags=["process"],
    summary="Traffic Noise Simulation"
)
async def process_job(
    calculation_input: Annotated[
        NoiseCalculationInput,
        Body(
            openapi_examples={
                "without_global_traffic_settings": {
                    "summary": "Without global traffic settings",
                    "description": "Max speed and traffic loads as stated in 'roads' parameter will not be changed",
                    "value": {
                        "buildings": load_json_file(BUILDINGS),
                        "roads": load_json_file(ROADS),
                    },
                },
                "global_traffic_settings": {
                    "summary": "Global traffic settings",
                    "description": "Max speed and traffic quota[%] will be applied to all roads \
                           with 'traffic_settings_adjustable' == true",
                    "value": {
                        "max_speed": 42,
                        "traffic_quota": 40,
                        "buildings": load_json_file(BUILDINGS),
                        "roads": load_json_file(ROADS),
                    },
                },
            }
        ),
    ]
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
