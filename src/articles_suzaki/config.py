from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class articles_config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parents[2] / ".env",
        extra="ignore",
    )

    guardian_api_key: str = Field(default="", alias="GUARDIAN_OPEN_PLATFORM_API")
    

    @property
    def project_root(self) -> Path:
        return Path(__file__).resolve().parents[2]



config = articles_config()
