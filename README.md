# PyCrawler - A Distributed Async Web Crawler Built in Python
A production-oriented scaffold for a distributed, asynchronous web crawler built with:
- Python 3.12+
- FastAPI (API + orchestration)
- aiohttp (async HTTP client)
- selectolax (fast HTML parsing)
- Redis (queue, visited set, rate-limiting)
- Docker + Docker Compose
- GitHub Actions CI + pytest, black, isort

This repo is a PR-ready scaffold (first iteration). It includes:
- API service (`app/main.py`) with endpoints:
  - POST /crawl → submit job
  - GET /status?job_id=... → job status
- Worker service to execute crawl jobs (distributed via Redis)
- Crawler engine (async, concurrency-limited, retry/backoff)
- Basic structured JSON logging & metrics hooks
- Docker Compose to run API + worker + Redis locally
- GitHub Actions CI to run tests & lint

## Quickstart (local via Docker Compose)
1. Copy `.env.example` -> `.env` and edit if needed.
2. Start services:
   ```bash
   docker compose up --build
   ```
3. Submit a crawl job:
   ```bash
   curl -X POST "http://localhost:8000/crawl" -H "Content-Type: application/json" -d '{"start_urls": ["https://example.com"], "max_depth": 1}'
   ```
4. Check status:
   ```bash
   curl "http://localhost:8000/status?job_id=<job-id>"
   ```

## Project layout (first iteration)
```
async-distributed-crawler/
├─ app/
│  ├─ main.py
│  ├─ api/
│  │  └─ routes.py
│  ├─ crawler/
│  │  ├─ engine.py
│  │  └─ worker.py
│  ├─ core/
│  │  └─ config.py
│  ├─ services/
│  │  └─ queue.py
│  ├─ models/
│  │  └─ schemas.py
│  └─ logging_config.py
├─ tests/
│  └─ test_api.py
├─ docker/
│  └─ Dockerfile
├─ docker-compose.yml
├─ requirements.txt
├─ .github/workflows/ci.yml
├─ README.md
```

## Notes, edge cases & trade-offs
- This scaffold favors clarity and testability. It uses Redis for coordination (queues + visited sets).
- Domain politeness (robots.txt) and advanced rate-limiting are _important_ — basic hooks are provided; implement full robots parsing in production.
- Error handling and per-domain backoff should be hardened for production.
- For huge-scale crawling (1000+ RPS), move workers into ECS/K8s, use connection pooling, and consider partitioning by domain to avoid hammering single hosts.

See `DESIGN.md` for the system design doc included in the repo.
