from pydantic import BaseSettings, Field, AnyUrl

class Settings(BaseSettings):
    REDIS_URL: AnyUrl = Field("redis://redis:6379/0")
    CRAWLER_CONCURRENCY: int = Field(50)
    CRAWLER_MAX_DEPTH: int = Field(2)
    JOB_TTL_SECONDS: int = Field(86400)

    # Robots settings
    USER_AGENT: str = Field("AsyncDistributedCrawler/1.0")
    ROBOTS_CACHE_TTL_SECONDS: int = Field(24 * 60 * 60)  # 24 hours

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()
