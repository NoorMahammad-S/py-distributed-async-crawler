import aiohttp
import asyncio
from typing import Optional
from app.rate_limiter.utils import domain_from_url
from app.core.config import settings
from app.robots.manager import RobotsManager
from app.rate_limiter.limiter import DomainRateLimiter
import logging

logger = logging.getLogger("fetcher")


class FetchDeniedByRobots(Exception):
    """Raised when robots.txt disallows fetching the URL."""


class Fetcher:
    """
    Fetcher that enforces robots.txt and domain rate limits.
    - Before fetching a URL:
      * check robots.txt (allow/disallow)
      * obtain crawl-delay (if any) and adapt rate limiter
      * acquire domain token via distributed rate limiter
    """

    def __init__(self, rate_limiter: DomainRateLimiter, robots: RobotsManager, user_agent: Optional[str] = None):
        self.rate_limiter = rate_limiter
        self.robots = robots
        self.user_agent = user_agent or settings.USER_AGENT
        self._session: Optional[aiohttp.ClientSession] = None

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(headers={"User-Agent": self.user_agent})
        return self._session

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def fetch(self, url: str, timeout: int = 10) -> Optional[str]:
        domain = domain_from_url(url)

        # 1) robots.txt enforcement
        allowed = await self.robots.is_allowed(url, self.user_agent)
        if not allowed:
            logger.info("Robots disallow fetching %s", url)
            raise FetchDeniedByRobots(f"Disallowed by robots.txt: {url}")

        # 2) check crawl-delay
        crawl_delay = await self.robots.get_crawl_delay(url, self.user_agent)
        # If crawl_delay is set, convert to refill_rate tokens/sec = 1 / crawl_delay
        # If crawl_delay is None or zero, we pass None and the rate_limiter will use defaults.
        refill_rate = None
        if crawl_delay and crawl_delay > 0:
            refill_rate = 1.0 / float(crawl_delay)

        # 3) acquire domain token from distributed rate limiter (blocks until allowed)
        # We keep capacity at 1 for strict crawl-delay semantics; keep defaults otherwise.
        try:
            await self.rate_limiter.acquire(domain, capacity=1, refill_rate=refill_rate)
        except Exception as e:
            logger.exception("Error acquiring rate limit token for %s: %s", domain, e)
            # Best-effort continue or treat as transient; choose to raise to be explicit
            raise

        # 4) perform HTTP fetch
        try:
            async with self.session.get(url, timeout=timeout) as resp:
                resp.raise_for_status()
                text = await resp.text()
                logger.debug("Fetched %s status=%s len=%d", url, resp.status, len(text) if text else 0)
                return text
        except Exception as e:
            logger.warning("Fetch error for %s: %s", url, e)
            return None
