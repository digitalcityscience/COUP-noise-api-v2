from pathlib import Path
from typing import Literal

from pydantic import Field

from noise_api.models.base import BaseModelStrict
from noise_api.utils import hash_dict, load_json_file

JSONS_DIR = Path(__file__).parent / "jsons"
BUILDINGS = JSONS_DIR / "buildings.json"
ROADS = JSONS_DIR / "roads.json"


class NoiseScenario(BaseModelStrict):
    max_speed: int = Field(..., ge=0, le=70, description="Maximum speed in km/h (0-70)")
    traffic_quota: int = Field(
        ..., ge=0, le=100, description="Traffic quota in percent (0-100)"
    )
    result_format: Literal["png", "geojson"] = "geojson"
    # TODO: conversion and option should be implemented at the CUT API level


class NoiseCalculationInput(NoiseScenario):
    buildings: dict
    roads: dict

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
                },
                "result_format": self.result_format,
            }
        )

    @property
    def celery_key(self) -> str:
        return f"{self.hash}_{self.scenario_hash}"
