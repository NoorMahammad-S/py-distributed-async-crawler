from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from typing import List, Optional
import uuid

from app.core.config import settings
from app.services.job_store import JobStore
from app.services.queue import RedisQueue

router = APIRouter()

class CrawlRequest(BaseModel):
    start_urls: List[str] = Field(..., min_items=1)
    max_depth: int = Field(1, ge=0)
    concurrency: Optional[int] = None

class CrawlResponse(BaseModel):
    job_id: str
    message: str

class StatusResponse(BaseModel):
    job_id: str
    created_at: int
    started_at: int
    finished_at: int
    runtime_seconds: int
    total_queued: int
    pending: int
    in_progress: int
    completed: int
    failed: int

@router.post("/crawl", response_model=CrawlResponse)
async def submit_crawl(req: CrawlRequest):
    job_id = str(uuid.uuid4())
    # create job record in redis
    job_store = JobStore(settings.REDIS_URL)
    await job_store.create_job(job_id=job_id, start_urls_count=len(req.start_urls), ttl_seconds=settings.JOB_TTL_SECONDS)

    payload = {
        "job_id": job_id,
        "start_urls": req.start_urls,
        "max_depth": req.max_depth,
        "concurrency": req.concurrency or settings.CRAWLER_CONCURRENCY,
    }
    q = RedisQueue(settings.REDIS_URL)
    await q.enqueue_job(payload)
    await job_store.close()
    return CrawlResponse(job_id=job_id, message="Job submitted")

@router.get("/status", response_model=StatusResponse)
async def get_status(job_id: str = Query(...)):
    job_store = JobStore(settings.REDIS_URL)
    stat = await job_store.get_status(job_id)
    await job_store.close()
    if stat is None:
        raise HTTPException(status_code=404, detail="job not found")
    return StatusResponse(**stat)
