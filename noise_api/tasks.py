from celery import signals
from celery.utils.log import get_task_logger

from noise_api.dependencies import cache, celery_app
from noise_api.noise_analysis.noisemap import run_noise_calculation

# from noise_api.models.calculation_input import NoiseTask


logger = get_task_logger(__name__)


@celery_app.task()
def compute_task(task_def: dict) -> dict:
    return run_noise_calculation(task_def)


@celery_app.task()
def find_result_in_cache(celery_key: str) -> dict | None:
    # Returns result dict or None
    return cache.get(key=celery_key)


@signals.task_postrun.connect
def task_postrun_handler(task_id, task, *args, **kwargs):

    if "find_result_in_cache" in task.name:
        # do not cache again
        return

    state = kwargs.get("state")
    args = kwargs.get("args")[0]
    result = kwargs.get("retval")

    if state == "SUCCESS":
        key = args["celery_key"]
        cache.put(key=key, value=result)
        logger.info(f"Saved result with key {key} to cache.")
