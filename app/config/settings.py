class Settings(BaseSettings):
    # ...
    RATE_LIMIT_DEFAULT_CAPACITY: int = 5       # max concurrent tokens per domain
    RATE_LIMIT_REFILL_RATE: float = 1.0        # 1 token per second
