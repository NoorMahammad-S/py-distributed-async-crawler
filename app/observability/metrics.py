"""
MetricsManager
- Provides Prometheus client metrics objects for local processes
- Writes aggregated counters to Redis for distributed-safe aggregation
- Exposes a helper to build a CollectorRegistry from the Redis aggregates for /metrics endpoint
"""
from __future__ import annotations
from typing import Optional, Dict, Any
import time
import logging

import redis.asyncio as aioredis
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

logger = logging.getLogger("metrics")


class MetricsManager:
    """
    Manages local Prometheus metrics and mirrors updates into Redis aggregates.
    Other workers/processes can write to the same Redis keys for a single aggregated view.
    """

    # Redis keys (hash)
    REDIS_KEY = "metrics:aggregates"

    def __init__(self, redis_url: str):
        self.redis = aioredis.from_url(redis_url, decode_responses=True)

        # local prometheus metrics (per-process)
        self.jobs_started = Counter("crawler_jobs_started_total", "Total jobs started")
        self.jobs_completed = Counter("crawler_jobs_completed_total", "Total jobs completed")
        self.jobs_failed = Counter("crawler_jobs_failed_total", "Total jobs failed")

        self.urls_processed = Counter("crawler_urls_processed_total", "Total URLs processed successfully")
        self.urls_failed = Counter("crawler_urls_failed_total", "Total URLs failed")

        self.jobs_in_progress = Gauge("crawler_jobs_in_progress", "Jobs currently in progress")
        self.urls_in_progress = Gauge("crawler_urls_in_progress", "URLs currently in progress")

        # Using a histogram to capture runtime distribution locally.
        self.job_runtime_hist = Histogram("crawler_job_runtime_seconds", "Job runtime seconds")

    # ---- Local updates  Redis aggregation helpers ----
    async def incr_job_started(self, n: int = 1) -> None:
        self.jobs_started.inc(n)
        self.jobs_in_progress.inc(n)
        await self._hincr("jobs_started_total", n)
        await self._hincr("jobs_in_progress", n)

    async def incr_job_completed(self, runtime: Optional[float] = None, n: int = 1) -> None:
        self.jobs_completed.inc(n)
        self.jobs_in_progress.dec(n)
        await self._hincr("jobs_completed_total", n)
        await self._hincr("jobs_in_progress", -n)
        if runtime is not None:
            self.job_runtime_hist.observe(runtime)
            # store sum  count for aggregated average/histogram approximations
            await self._hincrfloat("job_runtime_sum", float(runtime))
            await self._hincr("job_runtime_count", 1)

    async def incr_job_failed(self, n: int = 1) -> None:
        self.jobs_failed.inc(n)
        self.jobs_in_progress.dec(n)
        await self._hincr("jobs_failed_total", n)
        await self._hincr("jobs_in_progress", -n)

    async def incr_url_processed(self, n: int = 1) -> None:
        self.urls_processed.inc(n)
        await self._hincr("urls_processed_total", n)

    async def incr_url_failed(self, n: int = 1) -> None:
        self.urls_failed.inc(n)
        await self._hincr("urls_failed_total", n)

    async def set_urls_in_progress(self, value: int) -> None:
        self.urls_in_progress.set(value)
        # store gauge as integer
        await self.redis.hset(self.REDIS_KEY, "urls_in_progress", int(value))

    async def set_jobs_in_progress(self, value: int) -> None:
        self.jobs_in_progress.set(value)
        await self.redis.hset(self.REDIS_KEY, "jobs_in_progress", int(value))

    # ---- Redis helper low-level ops ----
    async def _hincr(self, field: str, n: int = 1) -> None:
        await self.redis.hincrby(self.REDIS_KEY, field, n)

    async def _hincrfloat(self, field: str, v: float) -> None:
        # Redis HINCRBYFLOAT returns new value
        await self.redis.hincrbyfloat(self.REDIS_KEY, field, v)

    async def get_aggregates(self) -> Dict[str, Any]:
        """
        Read aggregated metrics from Redis and return as dict.
        """
        data = await self.redis.hgetall(self.REDIS_KEY)
        # convert numeric fields
        out: Dict[str, Any] = {}
        for k, v in (data or {}).items():
            try:
                if "." in str(v):
                    out[k] = float(v)
                else:
                    out[k] = int(v)
            except Exception:
                out[k] = v
        return out

    async def build_registry_from_aggregates(self) -> CollectorRegistry:
        """
        Build a CollectorRegistry populated with aggregated values from Redis.
        This registry can be passed to prometheus_client.generate_latest for exposition.
        """
        aggregates = await self.get_aggregates()
        registry = CollectorRegistry()

        # counters
        c_jobs_started = Counter("crawler_jobs_started_total", "Total jobs started", registry=registry)
        c_jobs_completed = Counter("crawler_jobs_completed_total", "Total jobs completed", registry=registry)
        c_jobs_failed = Counter("crawler_jobs_failed_total", "Total jobs failed", registry=registry)
        c_urls_processed = Counter("crawler_urls_processed_total", "Total URLs processed", registry=registry)
        c_urls_failed = Counter("crawler_urls_failed_total", "Total URLs failed", registry=registry)

        # gauges
        g_jobs_in_progress = Gauge("crawler_jobs_in_progress", "Jobs in progress", registry=registry)
        g_urls_in_progress = Gauge("crawler_urls_in_progress", "URLs in progress", registry=registry)

        # histogram approximation: we cannot rebuild bucketed histogram from aggregates easily;
        # provide an approximation using summary (sum/count) to compute average; use Gauge to expose avg.
        g_job_runtime_avg = Gauge("crawler_job_runtime_seconds_avg", "Average job runtime seconds", registry=registry)
        # set values if present

        # set counter values via _value.set since we create fresh counters: use internal _value for Counter not public.
        # The recommended approach is to use Gauge for setting current values or use custom Collector.
        # We'll set via the `_value.set()` for counters (works with the pure python client).
        def _set_counter(c: Counter, field: str):
            v = aggregates.get(field)
            if v is not None:
                try:
                    c._value.set(float(v))
                except Exception:
                    pass

        _set_counter(c_jobs_started, "jobs_started_total")
        _set_counter(c_jobs_completed, "jobs_completed_total")
        _set_counter(c_jobs_failed, "jobs_failed_total")
        _set_counter(c_urls_processed, "urls_processed_total")
        _set_counter(c_urls_failed, "urls_failed_total")

        # set gauges
        if "jobs_in_progress" in aggregates:
            try:
                g_jobs_in_progress.set(int(aggregates["jobs_in_progress"]))
            except Exception:
                pass
        if "urls_in_progress" in aggregates:
            try:
                g_urls_in_progress.set(int(aggregates["urls_in_progress"]))
            except Exception:
                pass

        # job runtime average
        if "job_runtime_sum" in aggregates and "job_runtime_count" in aggregates and aggregates["job_runtime_count"] > 0:
            avg = float(aggregates["job_runtime_sum"]) / float(aggregates["job_runtime_count"])
            g_job_runtime_avg.set(avg)

        return registry

    async def metrics_response(self) -> bytes:
        """
        Convenience helper to build Prometheus exposition bytes for HTTP response.
        """
        registry = await self.build_registry_from_aggregates()
        return generate_latest(registry)

    async def close(self) -> None:
        await self.redis.close()
