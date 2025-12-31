import pytest
import asyncio

from redis.asyncio import Redis
from app.services.job_store import JobStore
from app.main import create_app
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_list_jobs_pagination_and_status():
    r = Redis(host="localhost", port=6379, decode_responses=True)
    job_store = JobStore("redis://localhost:6379/0")

    # cleanup index
    await r.delete("jobs:all")

    # create 5 jobs with different counters and statuses
    for i in range(1, 6):
        jid = f"job-list-{i}"
        await job_store.create_job(jid, start_urls_count=1, ttl_seconds=60)
        # mark started for odd jobs
        if i % 2 == 1:
            await job_store.mark_started(jid)
        # complete job 1 only, fail job 3
        if i == 1:
            await job_store.incr_completed(jid, 1)
            await job_store.mark_finished(jid)
        if i == 3:
            await job_store.incr_failed(jid, 1)
            await job_store.mark_finished(jid)

    app = create_app()
    async with AsyncClient(app=app, base_url="http://test") as client:
        # page 1, size 2
        resp = await client.get("/jobs?page=1&page_size=2")
        assert resp.status_code == 200
        data = resp.json()
        assert data["page"] == 1
        assert data["page_size"] == 2
        assert "total" in data
        assert len(data["items"]) <= 2

        # filter completed
        resp2 = await client.get("/jobs?status=completed")
        assert resp2.status_code == 200
        completed_items = resp2.json()["items"]
        # job 1 was completed
        assert any(it["job_id"] == "job-list-1" for it in completed_items)

    # cleanup
    for i in range(1, 6):
        await r.delete(f"job:job-list-{i}:meta", f"job:job-list-{i}:counters")
    await r.delete("jobs:all")
    await r.close()
    await job_store.close()
