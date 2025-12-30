"""
Structured JSON logger helper for job-level events.
All events are logged as a single JSON object string to root logger.
"""
from __future__ import annotations
import json
import time
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger("crawler")  # reuse root crawler logger configured by logging_config

def json_event(event: str, job_id: str, extra: Optional[Dict[str, Any]] = None) -> None:
    """
    Emit a structured JSON log for an event.
    event: short event name like 'job.started', 'job.url.completed'
    job_id: job identifier
    extra: any additional structured fields (url, duration, count, reason, etc.)
    """
    payload: Dict[str, Any] = {
        "ts": int(time.time()),
        "event": event,
        "job_id": job_id,
    }
    if extra:
        payload.update(extra)
    # Emit as one JSON message. The global logging formatter will wrap again, but this ensures fields exist.
    logger.info(json.dumps(payload))
