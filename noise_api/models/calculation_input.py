import hashlib
import json
from typing import Literal

from pydantic import Field

from noise_api.models.base import BaseModelStrict


def hash_dict(dict_) -> str:
    dict_str = json.dumps(dict_, sort_keys=True)
    return hashlib.md5(dict_str.encode()).hexdigest()


class NoiseScenario(BaseModelStrict):
    max_speed: int = Field(..., ge=0, le=70, description="Maximum speed in km/h (0-70)")
    traffic_quota: int = Field(
        ..., ge=0, le=100, description="Traffic quota in percent (0-100)"
    )
    result_format: Literal["png", "geojson"] = "geojson"


class NoiseCalculationInput(NoiseScenario):
    buildings: dict
    roads: dict


class NoiseTask(NoiseCalculationInput):
    @property
    def hash(self) -> str:
        return hash_dict({"buildings": self.buildings, "roads": self.roads})

    @property
    def scenario_hash(self) -> str:
        return hash_dict(
            {
                "traffic_settings": {
                    "max_speed": self.max_speed,
                    "traffic_quota": self.traffic_quota,
                },
                "result_format": self.result_format,
            }
        )

    @property
    def celery_key(self) -> str:
        return f"{self.hash}_{self.scenario_hash}"
