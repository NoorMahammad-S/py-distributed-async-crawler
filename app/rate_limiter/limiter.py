import time
from typing import Optional
from urllib.parse import urlparse

from redis.asyncio import Redis


class DomainRateLimiter:
    """
    Distributed token-bucket domain-level rate limiter.
    Each domain has:
      - capacity: max tokens
      - refill_rate: tokens per second
    """

    def __init__(self, redis: Redis, default_capacity: int = 5, default_refill_rate: float = 1.0):
        self.redis = redis
        self.default_capacity = default_capacity
        self.default_refill_rate = default_refill_rate

    @staticmethod
    def extract_domain(url: str) -> str:
        return urlparse(url).netloc

    async def _refill(self, domain: str, capacity: int, refill_rate: float):
        """
        Refill tokens based on elapsed time since last refill.
        """
        key_tokens = f"rate_limit:{domain}:tokens"
        key_last = f"rate_limit:{domain}:last_refill"

        now = time.time()
        last_refill = await self.redis.get(key_last)
        last_refill = float(last_refill) if last_refill else now

        elapsed = now - last_refill
        new_tokens = elapsed * refill_rate

        current_tokens = await self.redis.get(key_tokens)
        current_tokens = float(current_tokens) if current_tokens else capacity

        updated_tokens = min(capacity, current_tokens + new_tokens)

        # Write updated state
        pipe = self.redis.pipeline()
        pipe.set(key_tokens, updated_tokens)
        pipe.set(key_last, now)
        await pipe.execute()

        return updated_tokens

    async def acquire(self, domain: str, capacity: Optional[int] = None, refill_rate: Optional[float] = None):
        """
        Block until a token is available for this domain.
        """
        capacity = capacity or self.default_capacity
        refill_rate = refill_rate or self.default_refill_rate

        key_tokens = f"rate_limit:{domain}:tokens"

        while True:
            # Refill first
            tokens = await self._refill(domain, capacity, refill_rate)

            if tokens >= 1:
                # Atomically consume a token
                new_val = await self.redis.decrbyfloat(key_tokens, 1.0)
                if new_val >= 0:
                    return  # Token acquired successfully
            # Not enough tokens → sleep small interval
            await asyncio.sleep(0.05)
