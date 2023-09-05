from celery import Celery

from noise_api.cache import Cache
from noise_api.config import settings

cache = Cache(
    connection_config=settings.cache.connection,
    key_prefix=settings.cache.key_prefix,
    ttl_days=settings.cache.ttl_days,
)

celery_app = Celery(
    __name__, broker=settings.cache.broker_url, backend=settings.cache.result_backend
)
