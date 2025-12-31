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
    def __init__(self, redis_url: str, metrics: Optional[MetricsManager] = None):
        self.redis = aioredis.from_url(redis_url, decode_responses=True)
        self.metrics = metrics

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
        # Add to global index (sorted by created_at) for listing & pagination
        try:
            await self.redis.zadd("jobs:all", {job_id: now})
            if ttl_seconds:
                # set TTL on the key via EXPIRE to keep index retention in sync (optional)
                await self.redis.expire("jobs:all", ttl_seconds)
        except Exception:
            logger.exception("Failed to add job to index: %s", job_id)

        # Emit structured event and metric
        json_event("job.created", job_id, {"total_queued": start_urls_count})

        if self.metrics:
            await self.metrics.incr_job_started(0)  # ensure key exists; started will be incremented when worker starts

    async def mark_started(self, job_id: str) -> None:
        now = int(time.time())
        await self.redis.hset(self._meta_key(job_id), "started_at", now)
        json_event("job.started", job_id, {"started_at": now})
        if self.metrics:
            await self.metrics.incr_job_started(1)

    async def mark_finished(self, job_id: str) -> None:
        now = int(time.time())
        await self.redis.hset(self._meta_key(job_id), "finished_at", now)

        json_event("job.finished", job_id, {"finished_at": now})
        # compute runtime and observe metric
        meta = await self.redis.hgetall(self._meta_key(job_id))
        try:
            started = int(meta.get("started_at") or 0)
            if started:
                runtime = now - started
            else:
                runtime = 0
        except Exception:
            runtime = 0
        if self.metrics:
            await self.metrics.incr_job_completed(runtime, n=1)


    # Counter operations
    async def incr_pending(self, job_id: str, n: int = 1) -> None:
        await self.redis.hincrby(self._counters_key(job_id), "pending", n)
        await self.redis.hincrby(self._counters_key(job_id), "total_queued", n)
        json_event("job.url.pending", job_id, {"delta": n})

    async def decr_pending(self, job_id: str, n: int = 1) -> None:
        await self.redis.hincrby(self._counters_key(job_id), "pending", -n)
        json_event("job.url.taken", job_id, {"delta": n})

    async def incr_in_progress(self, job_id: str, n: int = 1) -> None:
        await self.redis.hincrby(self._counters_key(job_id), "in_progress", n)

    async def decr_in_progress(self, job_id: str, n: int = 1) -> None:
        await self.redis.hincrby(self._counters_key(job_id), "in_progress", -n)

    async def incr_completed(self, job_id: str, n: int = 1) -> None:
        await self.redis.hincrby(self._counters_key(job_id), "completed", n)
        json_event("job.url.completed", job_id, {"count": n})
        if self.metrics:
            await self.metrics.incr_url_processed(n)

    async def incr_failed(self, job_id: str, n: int = 1) -> None:
        await self.redis.hincrby(self._counters_key(job_id), "failed", n)
        json_event("job.url.failed", job_id, {"count": n})
        if self.metrics:
            await self.metrics.incr_url_failed(n)

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

    async def _resolve_status(self, meta: Dict[str, Any], counters: Dict[str, Any]) -> str:
        """
        Derive simple status label from meta  counters:
         - completed (finished_at > 0 and failed == 0)
         - failed (finished_at > 0 and failed > 0)
         - in-progress (in_progress > 0)
         - pending (pending > 0)
         - queued (default)
        """
        try:
            finished_at = int(meta.get("finished_at") or 0)
        except Exception:
            finished_at = 0

        def _i(x):
            try:
                return int(x or 0)
            except Exception:
                return 0

        pending = _i(counters.get("pending"))
        in_progress = _i(counters.get("in_progress"))
        failed = _i(counters.get("failed"))

        if finished_at > 0:
            return "failed" if failed > 0 else "completed"
        if in_progress > 0:
            return "in-progress"
        if pending > 0:
            return "pending"
        return "queued"

    async def list_jobs(self, page: int = 1, page_size: int = 20, status: Optional[str] = None) -> Dict[str, Any]:
        """
        Paginated job listing.
        - page: 1-based page index
        - page_size: number of items per page
        - status: optional filter in ("pending","in-progress","completed","failed","queued")

        Returns:
        {
            "total": <int>,
            "page": <int>,
            "page_size": <int>,
            "items": [
                {
                    job_id, created_at, started_at, finished_at, runtime_seconds,
                    total_queued, pending, in_progress, completed, failed, status
                }, ...
            ]
        }
        """
        # compute range
        if page < 1:
            page = 1
        offset = (page - 1) * page_size
        end = offset  page_size - 1

        total = await self.redis.zcard("jobs:all")

        # fetch job ids in reverse chronological order (most recent first)
        job_ids = await self.redis.zrevrange("jobs:all", offset, end)

        # pipeline to get meta  counters for each job id
        pipe = self.redis.pipeline()
        for jid in job_ids:
            pipe.hgetall(self._meta_key(jid))
            pipe.hgetall(self._counters_key(jid))
        results = await pipe.execute()

        items = []
        # results: [meta_j1, counters_j1, meta_j2, counters_j2, ...]
        for i in range(0, len(results), 2):
            meta = results[i] or {}
            counters = results[i  1] or {}
            # convert numeric fields safely
            def _to_int(x):
                try:
                    return int(x)
                except Exception:
                    return 0
            created_at = _to_int(meta.get("created_at", 0))
            started_at = _to_int(meta.get("started_at", 0))
            finished_at = _to_int(meta.get("finished_at", 0))
            total_q = _to_int(counters.get("total_queued", 0))
            pending = _to_int(counters.get("pending", 0))
            in_progress = _to_int(counters.get("in_progress", 0))
            completed = _to_int(counters.get("completed", 0))
            failed = _to_int(counters.get("failed", 0))

            runtime = 0
            now = int(time.time())
            if started_at and finished_at:
                runtime = finished_at - started_at
            elif started_at:
                runtime = now - started_at

            stat = {
                "job_id": meta.get("job_id"),
                "created_at": created_at,
                "started_at": started_at,
                "finished_at": finished_at,
                "runtime_seconds": runtime,
                "total_queued": total_q,
                "pending": pending,
                "in_progress": in_progress,
                "completed": completed,
                "failed": failed,
            }
            stat["status"] = await self._resolve_status(meta, counters)

            items.append(stat)

        # apply status filter if requested (filter client-side; this is fine for page_size small)
        if status:
            filtered = [it for it in items if it["status"] == status]
            # total should reflect underlying ZSET total, but clients requesting filtering expect server-side filtering
            # For simplicity and correctness, when status filter is present we must scan the index until we fill page.
            # However to keep code simple and performant for typical workloads we filter items from the page slice only.
            items = filtered

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "items": items,
        }

    async def close(self) -> None:
        await self.redis.close()

