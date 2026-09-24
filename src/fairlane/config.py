"""Application configuration using Pydantic Settings."""

from functools import lru_cache

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Fairlane central configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # General environment
    environment: str = "development"
    log_level: str = "INFO"

    # API Configuration
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # PostgreSQL Configuration
    postgres_user: str = "fairlane"
    postgres_password: str = "fairlane_secret"
    postgres_db: str = "fairlane"
    postgres_host: str = "localhost"
    postgres_port: int = 5433
    database_url_override: str | None = None

    # Redis Configuration
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: str | None = None
    redis_url_override: str | None = None

    # Redis Streams Configuration
    redis_stream_name: str = "tasks:stream"
    redis_consumer_group: str = "fairlane:workers"

    # Worker Configuration
    heartbeat_interval_seconds: int = 3
    heartbeat_ttl_seconds: int = 10

    @computed_field  # type: ignore[prop-decorator]
    @property
    def database_url(self) -> str:
        """Asynchronous database connection URL for SQLAlchemy + asyncpg."""
        if self.database_url_override:
            return self.database_url_override
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def sync_database_url(self) -> str:
        """Synchronous database connection URL for Alembic migrations."""
        if self.database_url_override:
            return self.database_url_override.replace("+asyncpg", "")
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def redis_url(self) -> str:
        """Redis connection URL."""
        if self.redis_url_override:
            return self.redis_url_override
        auth = f":{self.redis_password}@" if self.redis_password else ""
        return f"redis://{auth}{self.redis_host}:{self.redis_port}/{self.redis_db}"


@lru_cache
def get_settings() -> Settings:
    """Return a cached instance of application settings."""
    return Settings()


settings = get_settings()
