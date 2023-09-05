from typing import Literal, Optional

from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    title: str = Field(..., env="APP_TITLE")
    description: str = Field(..., env="APP_DESCRIPTION")
    version: str = Field(..., env="APP_VERSION")
    debug: bool = Field(..., env="DEBUG")
    environment: Optional[Literal["LOCALDEV", "PROD"]] = Field(..., env="ENVIRONMENT")


settings = Settings()
