"""
Crawler engine:
"""

import asyncio
import logging
from app.crawler.fetcher import Fetcher
from app.crawler.parser import Parser


class CrawlerEngine:
    def __init__(self, concurrency: int = 20):
        self.fetcher = Fetcher()
        self.parser = Parser()
        self.sem = asyncio.Semaphore(concurrency)

    async def crawl_url(self, url: str):
        async with self.sem:
            logging.info(f"Fetching: {url}")
            html = await self.fetcher.fetch(url)
            return self.parser.parse(html)

