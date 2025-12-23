from fastapi import APIRouter
from pydantic import BaseModel
from app import RedisQueue
from app import settings

router = APIRouter()
queue = RedisQueue(url=settings.redis_url)


class CrawlRequest(BaseModel):
    url: str
    depth: int = 1


class CrawlResponse(BaseModel):
    job_id: str


@router.post("/crawl", response_model=CrawlResponse)
async def submit_crawl(req: CrawlRequest):
    job_id = await queue.enqueue({"url": req.url, "depth": req.depth})
    return CrawlResponse(job_id=job_id)


@router.get("/status/{job_id}")
async def job_status(job_id: str):
    # will be extended
    return {"job_id": job_id, "status": "pending"}
