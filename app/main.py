"""
PyCrawler - A Distributed Async Web Crawler Built in Python
FastAPI application: provides /crawl and /status endpoints
"""
from fastapi import FastAPI, Response
from app.api.routes import router as api_router
from app.logging_config import configure_logging
from app.observability.metrics import MetricsManager
from app.core.config import settings
import asyncio

def create_app() -> FastAPI:
    configure_logging()
    app = FastAPI(title="Async Distributed Crawler (MVP)")
    app.include_router(api_router, prefix="")

    @app.on_event("startup")
    async def startup_event():
        # prepare a global metrics manager instance for API to use
        app.state.metrics = MetricsManager(settings.REDIS_URL)

    @app.on_event("shutdown")
    async def shutdown_event():
        if hasattr(app.state, "metrics"):
            await app.state.metrics.close()

    @app.get("/metrics")
    async def metrics_endpoint():
        # Build a registry from Redis aggregates and return exposition bytes
        if not hasattr(app.state, "metrics"):
            app.state.metrics = MetricsManager(settings.REDIS_URL)
        data = await app.state.metrics.metrics_response()
        return Response(content=data, media_type=CONTENT_TYPE_LATEST)

    return app

app = create_app()
