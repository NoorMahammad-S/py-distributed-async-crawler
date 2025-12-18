import json
import uuid
import aioredis


class RedisQueue:
    def __init__(self, url: str):
        self.url = url

    async def _conn(self):
        return await aioredis.from_url(self.url, decode_responses=True)

    async def enqueue(self, payload: dict) -> str:
        job_id = uuid.uuid4().hex
        payload["job_id"] = job_id
        redis = await self._conn()
        await redis.lpush("queue:crawl", json.dumps(payload))
        return job_id

    async def dequeue(self):
        redis = await self._conn()
        _, data = await redis.brpop("queue:crawl")
        return json.loads(data)
