"""
PyCrawler - A Distributed Async Web Crawler Built in Python
FastAPI application: provides /crawl and /status endpoints
"""
from fastapi import FastAPI
from app import init_routes
from app import configure_logging
from app import DomainRateLimiter

configure_logging()

app = FastAPI(title="Async Distributed Crawler")
init_routes(app)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.on_event("startup")
async def startup_event():
    redis = await get_redis()
    settings = get_settings()

    app.state.rate_limiter = DomainRateLimiter(
        redis=redis,
        default_capacity=settings.RATE_LIMIT_DEFAULT_CAPACITY,
        default_refill_rate=settings.RATE_LIMIT_REFILL_RATE
    )