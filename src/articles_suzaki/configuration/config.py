from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator
from datetime import datetime, timezone, timedelta

class articles_config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parents[3] / ".env",
        extra="ignore",
    )

    guardian_api_key: str = Field(default="", alias="GUARDIAN_OPEN_PLATFORM_API")
    database_url: str = Field(default="", alias="DATABASE_URL")
    postgres_db_user: str = Field(default="", alias="POSTGRESQL_DB_USER")
    postgres_db_password: str = Field(default="", alias="POSTGRESQL_DB_PASSWORD")
    postgres_db_host: str = Field(default="", alias="POSTGRESQL_DB_HOST")
    postgres_db_port: str = Field(default="", alias="POSTGRESQL_DB_PORT")
    postgres_db_name: str = Field(default="", alias="POSTGRESQL_DB_NAME")
    mongo_uri: str = Field(default="", alias="MONGO_URI")
    mongo_db_name: str = Field(default="", alias="MONGO_DB_NAME")
    mongo_coll_metadata: str = Field(default="", alias="MONGO_COLL_METADATA")
    mongo_coll_news: str = Field(default="", alias="MONGO_COLL_NEWS")
    
    # Data source configuration
    data_source: str = Field(default="api", alias="DATA_SOURCE")
    mock_total_articles: int = Field(default=1_000_000, alias="MOCK_TOTAL_ARTICLES")
    mock_batch_size: int = Field(default=10, alias="MOCK_BATCH_SIZE")
    mock_seed: int | None = Field(default=None, alias="MOCK_SEED")

    # Transformation configuration
    # Leave both dates empty for daily mode (yesterday + today, UTC)
    transform_from_date: datetime | None = Field(default=None, alias="TRANSFORM_FROM_DATE")
    transform_to_date: datetime | None = Field(default=None, alias="TRANSFORM_TO_DATE")
    transform_extract_batch_size: int = Field(default=100, alias="TRANSFORM_EXTRACT_BATCH_SIZE")
    transform_minibatch_size: int = Field(default=500, alias="TRANSFORM_MINIBATCH_SIZE")

    @field_validator("transform_from_date", "transform_to_date", mode="before")
    @classmethod
    def _empty_date_to_none(cls, value):
        return None if value in ("", None) else value

    @field_validator("transform_from_date", "transform_to_date")
    @classmethod
    def _date_to_utc(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @property
    def transform_range(self) -> tuple[datetime, datetime]:
        """Return (from, to) with `to` exclusive; defaults to yesterday + today."""
        if self.transform_from_date and self.transform_to_date:
            if self.transform_from_date >= self.transform_to_date:
                raise ValueError("TRANSFORM_FROM_DATE must be before TRANSFORM_TO_DATE")
            return self.transform_from_date, self.transform_to_date
        if self.transform_from_date or self.transform_to_date:
            raise ValueError(
                "Set both TRANSFORM_FROM_DATE and TRANSFORM_TO_DATE, or leave both empty"
            )
        today = self.current_time.replace(hour=0, minute=0, second=0, microsecond=0)
        return today - timedelta(days=1), today + timedelta(days=1)

    @property
    def project_root(self) -> Path:
        return Path(__file__).resolve().parents[2]

    @property
    def current_time(self) -> datetime:
        return datetime.now(timezone.utc)

    @property
    def prior_time(self) -> datetime:
        return self.current_time - timedelta(days=1)


config = articles_config()
