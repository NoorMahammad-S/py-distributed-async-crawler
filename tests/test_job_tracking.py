import pytest
import asyncio

from redis.asyncio import Redis
from app.services.job_store import JobStore


@pytest.mark.asyncio
async def test_job_lifecycle():
    redis = Redis(host="localhost", port=6379, decode_responses=True)
    job_store = JobStore("redis://localhost:6379/0")
    job_id = "test-job-1"

    # create job with 2 initial urls
    await job_store.create_job(job_id, start_urls_count=2, ttl_seconds=10)
    s = await job_store.get_status(job_id)
    assert s["total_queued"] == 2
    assert s["pending"] == 2
    assert s["in_progress"] == 0

    # mark started
    await job_store.mark_started(job_id)
    s = await job_store.get_status(job_id)
    assert s["started_at"] != 0

    # simulate processing one url
    await job_store.decr_pending(job_id, 1)
    await job_store.incr_in_progress(job_id, 1)
    s = await job_store.get_status(job_id)
    assert s["pending"] == 1
    assert s["in_progress"] == 1

    # complete it
    await job_store.incr_completed(job_id, 1)
    await job_store.decr_in_progress(job_id, 1)
    s = await job_store.get_status(job_id)
    assert s["completed"] == 1
    assert s["in_progress"] == 0

    # mark finished
    await job_store.mark_finished(job_id)
    s = await job_store.get_status(job_id)
    assert s["finished_at"] != 0

    # cleanup
    await job_store.close()
    await redis.delete(f"job:{job_id}:meta", f"job:{job_id}:counters")
    await redis.close()
