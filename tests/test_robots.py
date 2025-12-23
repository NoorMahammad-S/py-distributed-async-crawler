import asyncio
import pytest
from redis.asyncio import Redis
from app.robots.manager import RobotsManager


@pytest.mark.asyncio
async def test_robots_parse_and_enforce(monkeypatch):
    """
    Test that RobotsManager correctly parses robots.txt content and enforces rules.
    We monkeypatch _fetch_robots_content to avoid actual network calls.
    """

    # fake robots content for example.com
    fake_robots = """
    User-agent: *
    Disallow: /private
    Crawl-delay: 2
    """

    async def fake_fetch(domain):
        assert domain == "example.com"
        return fake_robots

    redis = Redis(host="localhost", port=6379, decode_responses=True)
    mgr = RobotsManager(redis_client=redis, ttl_seconds=10, user_agent="TestAgent")

    # monkeypatch the network fetch
    monkeypatch.setattr(mgr, "_fetch_robots_content", fake_fetch)

    # allowed URL
    ok = await mgr.is_allowed("https://example.com/public", user_agent="TestAgent")
    assert ok is True

    # disallowed URL
    ok2 = await mgr.is_allowed("https://example.com/private/page", user_agent="TestAgent")
    assert ok2 is False

    delay = await mgr.get_crawl_delay("https://example.com/private", user_agent="TestAgent")
    assert delay == 2.0

    # cleanup
    await mgr.invalidate("example.com")
    await redis.close()
