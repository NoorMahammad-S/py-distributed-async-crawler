"""
Redis-backed JobStore for job lifecycle and progress reporting.

Key schema:
 - job:{job_id}:meta      (hash)  => created_at, started_at, finished_at
 - job:{job_id}:counters  (hash)  => total_queued, pending, in_progress, completed, failed
 - jobs:all               (zset)  => global index by created_at
 - jobs:status:{status}   (zset)  => per-status index by created_at
"""

from __future__ import annotations
from typing import Optional, Dict, Any, List
import time
import logging

import redis.asyncio as aioredis

logger = logging.getLogger("jobstore")

# canonical statuses we maintain
STATUS_LIST = ["queued", "pending", "in-progress", "completed", "failed"]

class JobStore:
    def __init__(self, redis_url: str, metrics: Optional[Any] = None):
        self.redis = aioredis.from_url(redis_url, decode_responses=True)
        self.metrics = metrics

    def _meta_key(self, job_id: str) -> str:
        return f"job:{job_id}:meta"

    def _counters_key(self, job_id: str) -> str:
        return f"job:{job_id}:counters"

    def _status_zset_key(self, status: str) -> str:
        return f"jobs:status:{status}"


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

        # Add to global index (sorted by created_at) for listing & pagination
        try:
            await self.redis.zadd("jobs:all", {job_id: now})
            # Add to queued status zset as initial status
            await self.redis.zadd(self._status_zset_key("queued"), {job_id: now})
            if ttl_seconds:
                # optionally expire keys (best-effort)
                await self.redis.expire(meta_key, ttl_seconds)
                await self.redis.expire(counters_key, ttl_seconds)
        except Exception:
            logger.exception("Failed to add job to index: %s", job_id)

        # Emit structured event and metric
        json_event("job.created", job_id, {"total_queued": start_urls_count})

        if self.metrics:
            await self.metrics.incr_job_started(0)  # ensure key exists; started will be incremented when worker starts

    async def mark_started(self, job_id: str) -> None:
        now = int(time.time())
        await self.redis.hset(self._meta_key(job_id), "started_at", now)
        # Atomically move job to 'in-progress' status zset and remove from other status zsets
        try:
            pipe = self.redis.pipeline()
            for s in STATUS_LIST:
                pipe.zrem(self._status_zset_key(s), job_id)
            pipe.zadd(self._status_zset_key("in-progress"), {job_id: int(await self.redis.hget(self._meta_key(job_id), "created_at") or now)})
            await pipe.execute()
        except Exception:
            logger.exception("Failed to move job to in-progress: %s", job_id)

        json_event("job.started", job_id, {"started_at": now})
        if self.metrics:
            await self.metrics.incr_job_started(1)

    async def mark_finished(self, job_id: str) -> None:
        now = int(time.time())
        await self.redis.hset(self._meta_key(job_id), "finished_at", now)
        # Decide whether completed or failed based on counters
        counters = await self.redis.hgetall(self._counters_key(job_id))
        try:
            failed = int(counters.get("failed") or 0)
        except Exception:
            failed = 0
        target_status = "failed" if failed > 0 else "completed"
        # Atomically move to target_status zset and remove from others
        try:
            created_at = int((await self.redis.hget(self._meta_key(job_id), "created_at")) or now)
            pipe = self.redis.pipeline()
            for s in STATUS_LIST:
                pipe.zrem(self._status_zset_key(s), job_id)
            pipe.zadd(self._status_zset_key(target_status), {job_id: created_at})
            await pipe.execute()
        except Exception:
            logger.exception("Failed to move job to finished status: %s", job_id)

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


    # Counter operations update status zsets on change (best-effort)
    async def incr_pending(self, job_id: str, n: int = 1) -> None:
        await self.redis.hincrby(self._counters_key(job_id), "pending", n)
        await self.redis.hincrby(self._counters_key(job_id), "total_queued", n)
        # If pending >0 and not in-progress, ensure 'pending' set membership
        await self._ensure_status_based_on_counters(job_id)

        json_event("job.url.pending", job_id, {"delta": n})

    async def decr_pending(self, job_id: str, n: int = 1) -> None:
        await self.redis.hincrby(self._counters_key(job_id), "pending", -n)
        # update status sets
        await self._ensure_status_based_on_counters(job_id)

        json_event("job.url.taken", job_id, {"delta": n})

    async def incr_in_progress(self, job_id: str, n: int = 1) -> None:
        await self.redis.hincrby(self._counters_key(job_id), "in_progress", n)
        await self._ensure_status_based_on_counters(job_id)

    async def decr_in_progress(self, job_id: str, n: int = 1) -> None:
        await self.redis.hincrby(self._counters_key(job_id), "in_progress", -n)
        await self._ensure_status_based_on_counters(job_id)

    async def incr_completed(self, job_id: str, n: int = 1) -> None:
        await self.redis.hincrby(self._counters_key(job_id), "completed", n)
        json_event("job.url.completed", job_id, {"count": n})
        if self.metrics:
            await self.metrics.incr_url_processed(n)
        await self._ensure_status_based_on_counters(job_id)

    async def incr_failed(self, job_id: str, n: int = 1) -> None:
        await self.redis.hincrby(self._counters_key(job_id), "failed", n)
        json_event("job.url.failed", job_id, {"count": n})
        if self.metrics:
            await self.metrics.incr_url_failed(n)
        await self._ensure_status_based_on_counters(job_id)

    async def _ensure_status_based_on_counters(self, job_id: str) -> None:
        """
        Best-effort: inspect counters & meta and move job to an appropriate status zset.
        Priority:
         - if finished_at >0 -> completed/failed (do not override)
         - elif in_progress >0 -> in-progress
         - elif pending >0 -> pending
         - else -> queued
        This moves the job atomically by removing from all status zsets and adding to the target.
        """
        meta = await self.redis.hgetall(self._meta_key(job_id))
        counters = await self.redis.hgetall(self._counters_key(job_id))
        target = await self._derive_status_from_meta_counters(meta, counters)
        # compute created_at for score
        try:
            created_at = int(meta.get("created_at") or int(time.time()))
        except Exception:
            created_at = int(time.time())

        try:
            pipe = self.redis.pipeline()
            for s in STATUS_LIST:
                pipe.zrem(self._status_zset_key(s), job_id)
            pipe.zadd(self._status_zset_key(target), {job_id: created_at})
            await pipe.execute()
        except Exception:
            logger.exception("Failed to update status sets for job: %s", job_id)

    async def _derive_status_from_meta_counters(self, meta: Dict[str, Any], counters: Dict[str, Any]) -> str:
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

    async def get_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Return aggregated job status or None if job not found."""
        meta = await self.redis.hgetall(self._meta_key(job_id))
        if not meta:
            return None
        counters = await self.redis.hgetall(self._counters_key(job_id))

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

        status = await self._derive_status_from_meta_counters(meta, counters)

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
            "status": status,
        }

    async def list_jobs(self, page: int = 1, page_size: int = 20, status: Optional[str] = None) -> Dict[str, Any]:
        """
        Paginated job listing optimized to use per-status zset when status provided.
        """
        if page < 1:
            page = 1
        offset = (page - 1) * page_size
        end = offset + page_size - 1

        if status:
            if status not in STATUS_LIST:
                raise ValueError(f"unknown status: {status}")
            zkey = self._status_zset_key(status)
            total = await self.redis.zcard(zkey)
            job_ids = await self.redis.zrevrange(zkey, offset, end)
        else:
            total = await self.redis.zcard("jobs:all")
            job_ids = await self.redis.zrevrange("jobs:all", offset, end)

        # pipeline to fetch meta + counters
        pipe = self.redis.pipeline()
        for jid in job_ids:
            pipe.hgetall(self._meta_key(jid))
            pipe.hgetall(self._counters_key(jid))
        results = await pipe.execute()

        items = []
        for i in range(0, len(results), 2):
            meta = results[i] or {}
            counters = results[i + 1] or {}

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
            stat["status"] = await self._derive_status_from_meta_counters(meta, counters)

            items.append(stat)

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "items": items,
        }

    async def close(self) -> None:
        await self.redis.close()
