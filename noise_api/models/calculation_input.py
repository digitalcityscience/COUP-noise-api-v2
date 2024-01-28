from pathlib import Path
from typing import Optional

from pydantic import Field

from noise_api.models.base import BaseModelStrict
from noise_api.utils import hash_dict, load_json_file

JSONS_DIR = Path(__file__).parent / "jsons"
BUILDINGS = JSONS_DIR / "buildings.json"
ROADS = JSONS_DIR / "roads.json"


class NoiseCalculationInput(BaseModelStrict):
    buildings: dict
    roads: dict
    max_speed: Optional[int] = Field(
        None, ge=0, le=70, description="OPTIONAL: Maximum speed in km/h (0-70)"
    )
    traffic_quota: Optional[int] = Field(
        None, ge=0, le=100, description="OPTIONAL: Traffic quota in percent (0-100)"
    )

    class Config:
        schema_extra = {
            "example": {
                "max_speed": 42,
                "traffic_quota": 40,
                "buildings": load_json_file(BUILDINGS),
                "roads": load_json_file(ROADS),
            }
        }


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
                }
            }
        )

    @property
    def celery_key(self) -> str:
        return f"{self.hash}_{self.scenario_hash}"
