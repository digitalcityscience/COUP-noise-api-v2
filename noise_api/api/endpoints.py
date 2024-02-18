import os
import logging
from typing import Annotated

from celery.result import AsyncResult
from fastapi import APIRouter, Body, Response, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.openapi.utils import get_openapi

import noise_api.tasks as tasks
from noise_api.api.documentation import get_processes, get_conformance, get_landingpage_json, get_openapi_examples
from noise_api.dependencies import celery_app
from noise_api.models.calculation_input import NoiseCalculationInput, NoiseTask
from noise_api.models.job_status_info import StatusInfo

logger = logging.getLogger(__name__)

router = APIRouter(tags=["jobs"])


def generate_openapi_json():
    return get_openapi(title=os.environ["APP_TITLE"], version="1.0.0", routes=router.routes, openapi_version="3.0.0")


@router.get("/")
async def get_landing_page() -> dict:
    """
    OGC Processes 7.2 Retrieve the API Landing page | https://docs.ogc.org/is/18-062r2/18-062r2.html#toc23
    """
    return get_landingpage_json()


@router.get("/conformance")
async def get_conformances() -> dict:
    """
    OGC Processes 7.4 Declaration of conformances | https://docs.ogc.org/is/18-062r2/18-062r2.html#toc25
    """
    return get_conformance()


@router.get("/processes/{process_id}")
@router.get("/processes")
async def get_processes_json(process_id: str = None) -> dict:
    """
    OGC Processes 7.9 Process List https://docs.ogc.org/is/18-062r2/18-062r2.html#toc30
    OGC Processes 7.10 Process Description https://docs.ogc.org/is/18-062r2/18-062r2.html#toc31
    """
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
    summary="Traffic Noise Simulation",
    status_code=201
)
async def process_job(
        calculation_input: Annotated[
            NoiseCalculationInput,
            Body(
                openapi_examples=get_openapi_examples()
            )
        ],
        response: Response
):
    response_content = {
            "processID": "traffic-noise",
            "type": "process",
    }

    calculation_task = NoiseTask(**calculation_input.dict())
    if _result := tasks.find_result_in_cache(celery_key=calculation_task.celery_key):
        logger.info(
            f"Result already cached with key: {calculation_task.celery_key}"
        )

        # run fetching result as task with delay, to get a jobID (to keep the workflow)
        job_id = tasks.find_result_in_cache.delay(celery_key=calculation_task.celery_key).id

        response_content["jobID"] = job_id
        response_content["status"] = StatusInfo.SUCCESS.value

        return response_content

    logger.info(
        f"Result with key: {calculation_task.celery_key} not found in cache. Starting calculation ..."
    )
    result = tasks.compute_task.delay(jsonable_encoder(calculation_task))

    # OGC Processes Requirement 34 | /req/core/process-execute-success-async
    response_content["jobID"] = result.id
    response_content["status"] = StatusInfo.ACCEPTED.value
    response.headers["Location"] = f"/noise/jobs/{result.id}"

    return response_content


@router.get("/jobs/{job_id}/results")
async def get_job(job_id: str):
    async_result = AsyncResult(job_id, app=celery_app)

    if async_result.state == "PENDING":
        # OGC 7.13.3 Requirement 45 | https://docs.ogc.org/is/18-062r2/18-062r2.html#toc34
        raise HTTPException(404, detail="result not ready")

    if async_result.failed():
        # OGC 7.13.3 Requirement 46 | https://docs.ogc.org/is/18-062r2/18-062r2.html#toc34
        raise HTTPException(status_code=500, detail=str(async_result.get()))

    if async_result.successful():
        return {"result": async_result.get()}

    raise HTTPException(status_code=404, detail="no such job")


@router.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    async_result = AsyncResult(job_id, app=celery_app)

    response = {
        "type": "process",
        "jobID": job_id,
    }
    if async_result.state == "FAILURE":
        response["status"] = StatusInfo.FAILURE.value
        response["message"] = {str(async_result.get())}

        return response

    if async_result.state == "PENDING":
        response["status"] = StatusInfo.PENDING.value

    if async_result.state == "SUCCESS":
        response["status"] = StatusInfo.SUCCESS.value

    return response
