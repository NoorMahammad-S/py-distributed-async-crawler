import pytest
import asyncio
from redis.asyncio import Redis
from app import DomainRateLimiter


@pytest.mark.asyncio
async def test_domain_rate_limiter_basic():
    redis = Redis(host="localhost", port=6379, decode_responses=True)
    limiter = DomainRateLimiter(redis, default_capacity=1, default_refill_rate=0.5)

    # First acquire → should pass
    await limiter.acquire("example.com")

    # Second acquire immediately → should block at least 1 second (2 tokens/sec)
    start = asyncio.get_event_loop().time()
    await limiter.acquire("example.com")
    elapsed = asyncio.get_event_loop().time() - start

    assert elapsed >= 1.0
