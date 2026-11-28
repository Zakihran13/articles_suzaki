from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
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
