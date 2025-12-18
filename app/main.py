"""
PyCrawler - A Distributed Async Web Crawler Built in Python
FastAPI application: provides /crawl and /status endpoints
"""
from fastapi import FastAPI
from app.api.server import init_routes
from app.core.logging_config import configure_logging

configure_logging()

app = FastAPI(title="Async Distributed Crawler")
init_routes(app)


@app.get("/health")
async def health():
    return {"status": "ok"}
