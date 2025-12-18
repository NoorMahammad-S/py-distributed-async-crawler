import asyncio
import logging
from app.redis.queue import RedisQueue
from app.core.config import settings
from app.crawler.engine import CrawlerEngine


async def worker_loop():
    queue = RedisQueue(url=settings.redis_url)
    crawler = CrawlerEngine(concurrency=settings.max_concurrency)

    while True:
        job = await queue.dequeue()
        logging.info(f"Worker received job: {job}")
        result = await crawler.crawl_url(job["url"])
        logging.info(f"Result: {result['title']}")


if __name__ == "__main__":
    asyncio.run(worker_loop())
