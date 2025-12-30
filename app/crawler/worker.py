import asyncio
import logging
import redis.asyncio as aioredis

from app.services.queue import RedisQueue
from app.core.config import settings
from app.crawler.engine import Crawler
from app.rate_limiter.limiter import DomainRateLimiter
from app.robots.manager import RobotsManager
from app.services.job_store import JobStore
from app.observability.logger import json_event
from app.observability.metrics import MetricsManager

logger = logging.getLogger("worker")


async def run_worker():
    redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    q = RedisQueue(settings.REDIS_URL)

    robots = RobotsManager(redis_client=redis_client, ttl_seconds=settings.ROBOTS_CACHE_TTL_SECONDS)
    rate_limiter = DomainRateLimiter(redis_client, default_capacity=1, default_refill_rate=1.0)
    metrics = MetricsManager(settings.REDIS_URL)
    job_store = JobStore(settings.REDIS_URL, metrics=metrics)

    while True:
        job = await q.dequeue_job(timeout=5)
        if job is None:
            await asyncio.sleep(1)
            continue
        job_id = job["job_id"]
        logger.info("Picked job %s", job_id)
        json_event("job.picked", job_id, {"worker": "default"})
        await job_store.mark_started(job_id)
        crawler = Crawler(
            concurrency=job.get("concurrency", settings.CRAWLER_CONCURRENCY),
            max_depth=job.get("max_depth", settings.CRAWLER_MAX_DEPTH),
            robots_manager=robots,
            rate_limiter=rate_limiter,
            job_store=job_store,
            job_id=job_id,
        )
        await crawler.crawl(job.get("start_urls", []))
        await job_store.mark_finished(job_id)
        status = await job_store.get_status(job_id)
        await q.client.hset(f"job:{job_id}:status", mapping={"processed": status["completed"]})
        json_event("job.finished.summary", job_id, {"completed": status["completed"], "failed": status["failed"]})
        await metrics.close()
        logger.info("Finished job %s processed=%d", job_id, status["completed"])
