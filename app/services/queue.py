"""Simple Redis queue abstraction using redis.asyncio
Pushes job payloads and stores minimal job status counters.
"""
import json
from typing import Any, Dict, Optional
import asyncio
import redis.asyncio as aioredis

class RedisQueue:
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self._client: Optional[aioredis.Redis] = None

    @property
    def client(self) -> aioredis.Redis:
        if self._client is None:
            self._client = aioredis.from_url(self.redis_url, decode_responses=True)
        return self._client

    async def enqueue_job(self, payload: Dict[str, Any]) -> None:
        # job payload pushed into global job queue (workers BLPOP)
        await self.client.rpush("crawl:job_queue", json.dumps(payload))

    async def dequeue_job(self, timeout: int = 5) -> Optional[Dict[str, Any]]:
        res = await self.client.blpop("crawl:job_queue", timeout=timeout)
        if not res:
            return None
        _, raw = res
        return json.loads(raw)

    async def close(self) -> None:
        if self._client:
            await self._client.close()
