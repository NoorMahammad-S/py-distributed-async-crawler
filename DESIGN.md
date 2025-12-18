
# System Design — Async Distributed Web Crawler (v1)

## Goals
- High-throughput asynchronous crawling
- Distributed worker model with Redis for task distribution & dedupe
- Clean architecture for maintainability and testability
- Dockerized for easy local dev & production deploy

## Components
1. API (FastAPI)
   - Submit crawl jobs (job payload includes start URLs, depth, domain rate limits)
   - Status endpoint (reads progress from Redis)
2. Worker(s)
   - Stateless processes that fetch jobs from Redis queue
   - Each worker runs an async event loop using aiohttp
   - Concurrency controlled by an asyncio.Semaphore
3. Redis
   - `crawl:queue` (list) — job queue
   - `job:{id}:status` (hash) — status & counters
   - `visited` (set) — dedupe visited URLs (namespaced per job)
   - Optional: per-domain rate-limit keys / token buckets

## Data flow
1. API accepts `POST /crawl` with start URLs and pushes a job payload to Redis.
2. Worker BLPOP from Redis, claims the job, and begins crawling:
   - fetch URLs with concurrency and retries
   - parse HTML for links, enqueue new URLs up to `max_depth`
   - persist minimal metadata into Redis (or external DB in future)
   - update job status counters in Redis for monitoring
3. API `GET /status` reads Redis and returns progress metrics.

## Reliability & Fault-Tolerance
- Workers should checkpoint progress back into Redis (current queue, in-progress set).
- On worker crash, unacknowledged URLs remain in Redis; implement visibility timeouts if moving to message brokers like SQS.
- Retries with exponential backoff for transient errors.

## Scaling
- Horizontal scaling: add more worker instances.
- Use sharding: assign workers to domain ranges (hash by domain) to reduce cross-host contention.
- For extremely high throughput, partition Redis or use a more scalable queue (Kafka) with worker-side dedupe.

## Observability
- Structured JSON logs (stdout) for ingestion by logging systems.
- Metrics: total fetched, failures, avg latency, queue depth (exposed via Prometheus endpoint).
- Tracing: instrument aiohttp + FastAPI with OpenTelemetry if needed.

## Security & Etiquette
- Respect robots.txt and implement per-domain throttling.
- Limit concurrency per domain.
- Avoid scraping private or authenticated content without permission.

