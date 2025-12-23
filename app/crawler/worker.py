import asyncio
import logging

import redis.asyncio as aioredis

from app.services.queue import RedisQueue
from app.core.config import settings
from app.crawler.engine import Crawler
from app.rate_limiter.limiter import DomainRateLimiter
from app.robots.manager import RobotsManager

logger = logging.getLogger("worker")


async def run_worker():
    # create a shared redis client used for queue  robots cache  rate limiter
    redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    q = RedisQueue(settings.REDIS_URL)

    # instantiate managers
    robots = RobotsManager(redis_client=redis_client, ttl_seconds=settings.ROBOTS_CACHE_TTL_SECONDS)
    rate_limiter = DomainRateLimiter(redis_client, default_capacity=1, default_refill_rate=1.0)

    while True:
        job = await q.dequeue_job(timeout=5)
        if job is None:
            await asyncio.sleep(1)
            continue
        job_id = job["job_id"]
        logger.info("Picked job %s", job_id)
        # Crawler is unchanged but its Fetcher (used internally) should be wired to use robotsrate_limiter.
        crawler = Crawler(concurrency=job.get("concurrency", settings.CRAWLER_CONCURRENCY),
                          max_depth=job.get("max_depth", settings.CRAWLER_MAX_DEPTH),
                          robots_manager=robots,
                          rate_limiter=rate_limiter)
        await crawler.crawl(job.get("start_urls", []))
        # update job status (very basic)
        job_key = f"job:{job_id}:status"
        await q.client.hset(job_key, mapping={"processed": len(crawler.visited)})
        logger.info("Finished job %s processed=%d", job_id, len(crawler.visited))


def main():
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
