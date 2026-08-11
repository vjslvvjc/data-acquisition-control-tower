"""
Acquisition layer.

Every fetch returns an Envelope, never a bare string. If a payload reaches the
parser without provenance attached, we've already lost the ability to answer
"where did this number come from" three weeks later — which is the question that
actually gets asked.

Design rules:
  - Retry only what is worth retrying (429, 5xx, timeouts). A 404 is an answer.
  - Back off exponentially with jitter, so a shared upstream doesn't get a
    synchronised stampede from every worker at once.
  - Respect robots.txt and a per-host rate limit by default. Both are
    overridable per source, but the override has to be explicit and recorded.
  - Hash the body. Unchanged hash across runs is a signal in itself: either the
    source is genuinely static or we're being served a cache.
"""

from __future__ import annotations

import hashlib
import logging
import random
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Mapping
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests

log = logging.getLogger(__name__)

USER_AGENT = "research-insights-bot/0.3 (+contact: data-team@example.com)"
RETRY_STATUS = {408, 425, 429, 500, 502, 503, 504}


@dataclass(frozen=True)
class Envelope:
    """A payload plus everything needed to defend it later."""

    source_id: str
    url: str
    body: str
    status: int
    retrieved_at: str
    latency_ms: int
    content_hash: str
    attempts: int
    from_cache: bool = False
    headers: Mapping[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class RateLimiter:
    """Token-bucket, one bucket per host. Thread-safe."""

    def __init__(self, per_second: float = 1.0, burst: int = 3) -> None:
        self._rate = per_second
        self._burst = burst
        self._tokens: dict[str, float] = {}
        self._last: dict[str, float] = {}
        self._lock = threading.Lock()

    def acquire(self, host: str) -> float:
        with self._lock:
            now = time.monotonic()
            last = self._last.get(host, now)
            tokens = min(self._burst, self._tokens.get(host, self._burst) + (now - last) * self._rate)

            if tokens < 1.0:
                wait = (1.0 - tokens) / self._rate
                self._tokens[host] = 0.0
                self._last[host] = now + wait
            else:
                wait = 0.0
                self._tokens[host] = tokens - 1.0
                self._last[host] = now

        if wait > 0:
            log.debug("rate limit: sleeping %.2fs for %s", wait, host)
            time.sleep(wait)
        return wait


class RobotsCache:
    """Fetch and cache robots.txt per host. Fail open, but say so loudly."""

    def __init__(self, ttl_seconds: int = 3600) -> None:
        self._cache: dict[str, tuple[RobotFileParser, float]] = {}
        self._ttl = ttl_seconds
        self._lock = threading.Lock()

    def allowed(self, url: str, user_agent: str = USER_AGENT) -> bool:
        parsed = urlparse(url)
        host = f"{parsed.scheme}://{parsed.netloc}"

        with self._lock:
            entry = self._cache.get(host)
            if entry and (time.monotonic() - entry[1]) < self._ttl:
                parser = entry[0]
            else:
                parser = RobotFileParser()
                parser.set_url(f"{host}/robots.txt")
                try:
                    parser.read()
                except Exception as exc:  # noqa: BLE001 - network shape varies
                    log.warning("robots.txt unreadable for %s (%s) — proceeding, flagged", host, exc)
                    return True
                self._cache[host] = (parser, time.monotonic())

        return parser.can_fetch(user_agent, url)


class Acquirer:
    def __init__(
        self,
        rate_limiter: RateLimiter | None = None,
        robots: RobotsCache | None = None,
        timeout: float = 15.0,
        max_attempts: int = 4,
    ) -> None:
        self.limiter = rate_limiter or RateLimiter()
        self.robots = robots or RobotsCache()
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"})

    def fetch(
        self,
        source_id: str,
        url: str,
        *,
        ignore_robots: bool = False,
        headers: Mapping[str, str] | None = None,
    ) -> Envelope:
        if not ignore_robots and not self.robots.allowed(url):
            raise PermissionError(f"{source_id}: robots.txt disallows {url}")
        if ignore_robots:
            log.warning("%s: robots.txt bypass explicitly enabled for %s", source_id, url)

        host = urlparse(url).netloc
        started = time.monotonic()
        last_error: Exception | None = None

        for attempt in range(1, self.max_attempts + 1):
            self.limiter.acquire(host)
            try:
                response = self.session.get(url, timeout=self.timeout, headers=dict(headers or {}))
            except requests.RequestException as exc:
                last_error = exc
                if attempt == self.max_attempts:
                    break
                self._backoff(attempt, reason=type(exc).__name__)
                continue

            if response.status_code in RETRY_STATUS and attempt < self.max_attempts:
                self._backoff(
                    attempt,
                    reason=f"HTTP {response.status_code}",
                    retry_after=response.headers.get("Retry-After"),
                )
                continue

            response.raise_for_status()
            body = response.text
            return Envelope(
                source_id=source_id,
                url=url,
                body=body,
                status=response.status_code,
                retrieved_at=datetime.now(timezone.utc).isoformat(),
                latency_ms=int((time.monotonic() - started) * 1000),
                content_hash=hashlib.sha256(body.encode("utf-8")).hexdigest()[:16],
                attempts=attempt,
                headers={k.lower(): v for k, v in response.headers.items() if k.lower() in
                         {"content-type", "last-modified", "etag", "x-ratelimit-remaining"}},
            )

        raise RuntimeError(
            f"{source_id}: exhausted {self.max_attempts} attempts against {url}"
        ) from last_error

    def _backoff(self, attempt: int, *, reason: str, retry_after: str | None = None) -> None:
        if retry_after:
            try:
                delay = float(retry_after)
                log.info("backoff: honouring Retry-After %.1fs (%s)", delay, reason)
                time.sleep(delay)
                return
            except ValueError:
                pass
        delay = min(2 ** attempt, 30) * (0.5 + random.random())
        log.info("backoff: attempt %d failed (%s), sleeping %.1fs", attempt, reason, delay)
        time.sleep(delay)
