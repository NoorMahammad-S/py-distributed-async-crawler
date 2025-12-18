from pydantic import BaseSettings


class Settings(BaseSettings):
    redis_url: str = "redis://redis:6379/0"
    max_concurrency: int = 50

    class Config:
        env_file = ".env"


settings = Settings()

