"""
Redis-backed JobStore for job lifecycle and progress reporting.

Key schema:
 - job:{job_id}:meta      (hash)  => created_at, started_at, finished_at
 - job:{job_id}:counters  (hash)  => total_queued, pending, in_progress, completed, failed
 - job:{job_id}:retention (optional TTL handling)

All methods are async and safe to call from multiple workers.
"""
from __future__ import annotations

from typing import Optional, Dict, Any
import time
import json
import logging

import redis.asyncio as aioredis

logger = logging.getLogger("jobstore")


class JobStore:
    def __init__(self, redis_url: str):
        self.redis = aioredis.from_url(redis_url, decode_responses=True)

    def _meta_key(self, job_id: str) -> str:
        return f"job:{job_id}:meta"

    def _counters_key(self, job_id: str) -> str:
        return f"job:{job_id}:counters"

    async def create_job(self, job_id: str, start_urls_count: int = 0, ttl_seconds: Optional[int] = None) -> None:
        now = int(time.time())
        meta_key = self._meta_key(job_id)
        counters_key = self._counters_key(job_id)

        # initialize meta
        await self.redis.hset(meta_key, mapping={
            "job_id": job_id,
            "created_at": now,
            "started_at": 0,
            "finished_at": 0,
        })

        # initialize counters
        await self.redis.hset(counters_key, mapping={
            "total_queued": int(start_urls_count),
            "pending": int(start_urls_count),
            "in_progress": 0,
            "completed": 0,
            "failed": 0,
        })

        if ttl_seconds:
            await self.redis.expire(meta_key, ttl_seconds)
            await self.redis.expire(counters_key, ttl_seconds)

    async def mark_started(self, job_id: str) -> None:
        now = int(time.time())
        await self.redis.hset(self._meta_key(job_id), "started_at", now)

    async def mark_finished(self, job_id: str) -> None:
        now = int(time.time())
        await self.redis.hset(self._meta_key(job_id), "finished_at", now)

    # Counter operations
    async def incr_pending(self, job_id: str, n: int = 1) -> None:
        await self.redis.hincrby(self._counters_key(job_id), "pending", n)
        await self.redis.hincrby(self._counters_key(job_id), "total_queued", n)

    async def decr_pending(self, job_id: str, n: int = 1) -> None:
        await self.redis.hincrby(self._counters_key(job_id), "pending", -n)

    async def incr_in_progress(self, job_id: str, n: int = 1) -> None:
        await self.redis.hincrby(self._counters_key(job_id), "in_progress", n)

    async def decr_in_progress(self, job_id: str, n: int = 1) -> None:
        await self.redis.hincrby(self._counters_key(job_id), "in_progress", -n)

    async def incr_completed(self, job_id: str, n: int = 1) -> None:
        await self.redis.hincrby(self._counters_key(job_id), "completed", n)

    async def incr_failed(self, job_id: str, n: int = 1) -> None:
        await self.redis.hincrby(self._counters_key(job_id), "failed", n)

    async def get_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Return aggregated job status or None if job not found."""
        meta = await self.redis.hgetall(self._meta_key(job_id))
        if not meta:
            return None
        counters = await self.redis.hgetall(self._counters_key(job_id))
        # convert numeric fields
        def _i(x):
            try:
                return int(x)
            except Exception:
                return 0

        created_at = _i(meta.get("created_at", 0))
        started_at = _i(meta.get("started_at", 0))
        finished_at = _i(meta.get("finished_at", 0))

        total = _i(counters.get("total_queued", 0))
        pending = _i(counters.get("pending", 0))
        in_progress = _i(counters.get("in_progress", 0))
        completed = _i(counters.get("completed", 0))
        failed = _i(counters.get("failed", 0))

        runtime = None
        now = int(time.time())
        if started_at and finished_at:
            runtime = finished_at - started_at
        elif started_at:
            runtime = now - started_at
        else:
            runtime = 0

        return {
            "job_id": job_id,
            "created_at": created_at,
            "started_at": started_at,
            "finished_at": finished_at,
            "runtime_seconds": runtime,
            "total_queued": total,
            "pending": pending,
            "in_progress": in_progress,
            "completed": completed,
            "failed": failed,
        }

    async def close(self) -> None:
        await self.redis.close()

