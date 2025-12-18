from fastapi import FastAPI
from app.api.routes.crawl import router as crawl_router


def init_routes(app: FastAPI):
    app.include_router(crawl_router)
