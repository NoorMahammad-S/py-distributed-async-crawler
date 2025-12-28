"""
Crawler engine:
- Uses Fetcher which enforces robots.txt and rate limiting
- Concurrency via asyncio.Semaphore
- parse HTML with selectolax
"""
from typing import List, Set, Optional
import asyncio
import logging
from urllib.parse import urljoin, urldefrag, urlparse

from selectolax.parser import HTMLParser

from app.crawler.fetcher import Fetcher, FetchDeniedByRobots
from app.rate_limiter.limiter import DomainRateLimiter
from app.robots.manager import RobotsManager

logger = logging.getLogger("crawler")


class Crawler:
    def __init__(self, concurrency: int = 10, max_depth: int = 1,
                 robots_manager: Optional[RobotsManager] = None,
                 rate_limiter: Optional[DomainRateLimiter] = None):
                 job_store: Optional["JobStore"] = None,
                 job_id: Optional[str] = None):

        self.concurrency = concurrency
        self.max_depth = max_depth
        self.semaphore = asyncio.Semaphore(concurrency)
        self.visited: Set[str] = set()
        self.to_fetch = asyncio.Queue()
        self.robots_manager = robots_manager
        self.rate_limiter = rate_limiter
        self.job_store = job_store
        self.job_id = job_id

        # Fetcher depends on rate_limiter & robots_manager
        if rate_limiter and robots_manager:
            self.fetcher = Fetcher(rate_limiter=rate_limiter, robots=robots_manager)
        else:
            # fallback to a minimal fetcher (no robots/rate limiting) for tests/dev
            self.fetcher = Fetcher(rate_limiter=DomainRateLimiter(aioredis.from_url("redis://localhost:6379", decode_responses=True)),
                                   robots=robots_manager or RobotsManager(aioredis.from_url("redis://localhost:6379", decode_responses=True)))

    def extract_links(self, base_url: str, html: str) -> List[str]:
        parsed = HTMLParser(html)
        urls: List[str] = []
        for node in parsed.css("a"):
            href = node.attributes.get("href") or ""
            try:
                joined = urljoin(base_url, href)
                clean, _ = urldefrag(joined)
                p = urlparse(clean)
                if p.scheme not in ("http", "https"):
                    continue
                urls.append(clean)
            except Exception:
                continue
        return urls

    async def crawl(self, start_urls: List[str]):
        if self.job_store and self.job_id:
            await self.job_store.incr_pending(self.job_id, len(start_urls))

        for u in start_urls:
            await self.to_fetch.put((u, 0))
        workers = [asyncio.create_task(self._worker()) for _ in range(self.concurrency)]
        await self.to_fetch.join()
        for w in workers:
            w.cancel()

    async def _process_url(self, url: str, depth: int):
        """
        Process a single URL: update job counters during lifecycle.
        """
        if self.job_store and self.job_id:
            # move pending -> in_progress
            await self.job_store.decr_pending(self.job_id, 1)
            await self.job_store.incr_in_progress(self.job_id, 1)
        try:
            html = await self.fetcher.fetch(url)
            if html:
                if self.job_store and self.job_id:
                    await self.job_store.incr_completed(self.job_id, 1)
                # discover links and add to queue
                links = self.extract_links(url, html)
                if links:
                    await self.job_store.incr_pending(self.job_id, len(links))
                    for link in links:
                        await self.to_fetch.put((link, depth  1))
            else:
                if self.job_store and self.job_id:
                    await self.job_store.incr_failed(self.job_id, 1)
        except Exception:
            if self.job_store and self.job_id:
                await self.job_store.incr_failed(self.job_id, 1)
        finally:
            if self.job_store and self.job_id:
                await self.job_store.decr_in_progress(self.job_id, 1)


    async def _worker(self):
        while True:
            url, depth = await self.to_fetch.get()
            try:
                if url in self.visited or depth > self.max_depth:
                    continue
                self.visited.add(url)
                await self._process_url(url, depth)
            finally:
                self.to_fetch.task_done()

