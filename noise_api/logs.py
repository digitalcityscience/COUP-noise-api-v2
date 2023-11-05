import dataclasses
import datetime
import enum
import json
import logging.config
import uuid
from typing import Any

from pydantic import BaseModel

from noise_api.config import settings


def setup_logging():
    logging.captureWarnings(True)
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "structured_log_formatter": {
                    "class": "noise_api.logs.StructuredLogFormatter"
                },
            },
            "handlers": {
                "default": {
                    "level": settings.log_level,
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                    "formatter": "structured_log_formatter",
                }
            },
            "loggers": {
                "root": {
                    "handlers": ["default"],
                    "level": settings.log_level,
                }
            },
        }
    )


class StructuredLogJSONEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, (datetime.datetime, datetime.date)):
            return o.isoformat()
        if isinstance(o, uuid.UUID):
            return str(o)
        if isinstance(o, enum.Enum):
            return o.value
        if isinstance(o, BaseModel):
            return o.dict()
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)

        return super().default(o)


class StructuredLogFormatter(logging.Formatter):
    _log_record_fields = [
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
    ]

    def format(self, record) -> str:
        return json.dumps(
            {
                "message": record.msg,
                "extra": {
                    k: v
                    for k, v in record.__dict__.items()
                    if k not in self._log_record_fields
                },
                "metadata": {
                    "created_at": datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat(),
                    "logger_name": record.name,
                    "log_level": record.levelname,
                    "pathname": record.pathname,
                    "lineno": record.lineno,
                    "exc_info": record.exc_info
                    and self.formatException(record.exc_info),
                    "stack_info": record.stack_info
                    and self.formatStack(record.stack_info),
                },
            },
            cls=StructuredLogJSONEncoder,
        )
