"""
Robots.txt manager
------------------
- Asynchronously fetches robots.txt per-domain
- Parses using urllib.robotparser.RobotFileParser
- Caches raw robots.txt in Redis with TTL (distributed cache)
- Exposes convenience APIs:
    - is_allowed(user_agent, url) -> bool
    - get_crawl_delay(user_agent, url) -> Optional[float]

Design notes:
- We store the RAW robots.txt body in Redis under key: robots:{domain}:raw
- We store a fetched timestamp key optionally (for debugging) as robots:{domain}:fetched_at
- TTL is configurable (defaults to 24h). Re-fetch happens after TTL expiry.
"""
from __future__ import annotations

import time
from typing import Optional
from urllib.parse import urljoin, urlparse
import logging
import asyncio

from urllib import robotparser
import aiohttp
import redis.asyncio as aioredis

from app.core.config import settings

logger = logging.getLogger("robots")


class RobotsManager:
    """
    Distributed Robots.txt manager backed by Redis.
    """

    def __init__(self, redis_client: aioredis.Redis, ttl_seconds: int = None, user_agent: str | None = None):
        self.redis = redis_client
        self.ttl_seconds = ttl_seconds or settings.ROBOTS_CACHE_TTL_SECONDS
        self.user_agent = user_agent or settings.USER_AGENT
        # local (in-process) parser cache for speed (domain -> (parser, fetched_at))
        self._local_cache: dict[str, tuple[robotparser.RobotFileParser, float]] = {}
        # per-domain lock to prevent thundering-herd when fetching robots
        self._locks: dict[str, asyncio.Lock] = {}

    @staticmethod
    def _domain_from_url(url: str) -> str:
        p = urlparse(url)
        return p.netloc.lower()

    async def _fetch_robots_content(self, domain: str) -> Optional[str]:
        """
        Fetch robots.txt from the domain root asynchronously.
        Returns the content string, or None if fetching failed (404 treated as empty).
        """
        url = urljoin(f"https://{domain}", "/robots.txt")
        try:
            async with aiohttp.ClientSession(headers={"User-Agent": self.user_agent}) as session:
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        return text
                    elif resp.status in (403, 404):
                        # treat 404/403 as "no robots" (empty rules — allow all)
                        logger.debug("robots.txt returned %s for %s", resp.status, domain)
                        return ""
                    else:
                        logger.warning("Unexpected status %s when fetching robots.txt for %s", resp.status, domain)
                        return None
        except Exception as e:
            logger.exception("Failed to fetch robots.txt for %s: %s", domain, e)
            return None

    async def _get_raw_cached(self, domain: str) -> Optional[str]:
        key = f"robots:{domain}:raw"
        raw = await self.redis.get(key)
        return raw  # may be None or empty string

    async def _set_raw_cached(self, domain: str, raw: str) -> None:
        key = f"robots:{domain}:raw"
        await self.redis.set(key, raw if raw is not None else "", ex=self.ttl_seconds)
        await self.redis.set(f"robots:{domain}:fetched_at", int(time.time()), ex=self.ttl_seconds)

    async def _get_parser(self, domain: str) -> Optional[robotparser.RobotFileParser]:
        """
        Returns a RobotFileParser for the domain. Uses local in-memory cache
        first (fast), falls back to Redis cache, and fetches remote robots.txt if needed.
        """
        now = time.time()
        # Check local cache
        entry = self._local_cache.get(domain)
        if entry:
            parser, fetched_at = entry
            if now - fetched_at < self.ttl_seconds:
                return parser

        # ensure single fetch per domain
        lock = self._locks.setdefault(domain, asyncio.Lock())
        async with lock:
            # re-check local cache while waiting for lock
            entry = self._local_cache.get(domain)
            if entry:
                parser, fetched_at = entry
                if now - fetched_at < self.ttl_seconds:
                    return parser

            # check Redis cache (raw robots)
            raw = await self._get_raw_cached(domain)
            if raw is None:
                # not cached or expired -> fetch remote
                raw = await self._fetch_robots_content(domain)
                # If fetch failed (None), we avoid caching to allow future retry
                if raw is None:
                    return None
                # store raw in Redis (may be empty string meaning "no robots")
                await self._set_raw_cached(domain, raw)

            # Build parser from raw content (robotparser expects lines)
            parser = robotparser.RobotFileParser()
            try:
                if raw == "":
                    # empty robots = allow all
                    parser.parse([])
                else:
                    lines = raw.splitlines()
                    parser.parse(lines)
                # cache in local memory
                self._local_cache[domain] = (parser, time.time())
                return parser
            except Exception:
                logger.exception("Failed to parse robots.txt for domain=%s", domain)
                return None

    async def is_allowed(self, url: str, user_agent: str | None = None) -> bool:
        """
        Check whether the given URL is allowed for the configured user_agent.
        If robots fetch fails (network), we conservatively allow (best-effort).
        """
        ua = user_agent or self.user_agent
        domain = self._domain_from_url(url)
        parser = await self._get_parser(domain)
        if parser is None:
            # fetch failed or parser error -> best-effort allow (do not block operations)
            # NOTE: conservative alternative is to block (deny) — code can be changed based on policy.
            logger.debug("No parser for %s; allowing by default", domain)
            return True
        return parser.can_fetch(ua, url)

    async def get_crawl_delay(self, url: str, user_agent: str | None = None) -> Optional[float]:
        """
        Return crawl-delay in seconds for the user-agent if present in robots rules.
        If not specified, returns None.
        """
        ua = user_agent or self.user_agent
        domain = self._domain_from_url(url)
        parser = await self._get_parser(domain)
        if parser is None:
            return None
        try:
            delay = parser.crawl_delay(ua)
            # parser.crawl_delay may return float or None
            return float(delay) if delay is not None else None
        except Exception:
            logger.exception("Error obtaining crawl-delay for %s", domain)
            return None

    async def invalidate(self, domain: str) -> None:
        """
        Remove cached robots for domain (both local and Redis).
        """
        self._local_cache.pop(domain, None)
        await self.redis.delete(f"robots:{domain}:raw", f"robots:{domain}:fetched_at")

    async def clear_local_cache(self) -> None:
        self._local_cache.clear()
