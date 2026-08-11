"""
Monitoring layer.

The premise: an extraction pipeline that fails loudly is a nuisance, and one
that fails quietly is a liability. Everything here exists to make sure the
second case cannot happen.

Three mechanisms:

  Heartbeat    Every source writes a timestamp on every successful run. An
               absent heartbeat is itself an alert — a pipeline that stops
               being scheduled produces no errors at all, which is exactly why
               nobody notices for a fortnight.

  Health state Sources move through HEALTHY -> DEGRADED -> FAILED and back.
               Transitions are alertable; steady states are not. This is what
               stops a source that's been broken for a week from paging
               somebody every fifteen minutes.

  Alert dedup  Same source, same code, inside the cooldown -> suppressed and
               counted, not re-sent. Alert fatigue is how real alerts get
               ignored.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Protocol

log = logging.getLogger(__name__)


class Health(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    STALE = "stale"


@dataclass
class RunRecord:
    run_id: str
    source_id: str
    started_at: str
    duration_ms: int
    records_acquired: int
    records_clean: int
    records_quarantined: int
    health: str
    parser_rule: str | None
    drift: bool
    content_hash: str | None
    error: str | None = None


class AlertSink(Protocol):
    def send(self, source_id: str, code: str, severity: str, message: str) -> None: ...


class LogSink:
    """Default sink. Replace with Slack/PagerDuty/n8n webhook in deployment."""

    def send(self, source_id: str, code: str, severity: str, message: str) -> None:
        log.log(
            {"info": logging.INFO, "warn": logging.WARNING, "error": logging.ERROR}.get(severity, logging.INFO),
            "ALERT [%s] %s/%s — %s", severity.upper(), source_id, code, message,
        )


class WebhookSink:
    """
    Posts to an n8n / Make webhook, which then fans out to Slack, email or a
    ticket. Keeping the fan-out in the automation platform rather than in this
    codebase means routing changes don't need a deploy.
    """

    def __init__(self, url: str, timeout: float = 5.0) -> None:
        self.url = url
        self.timeout = timeout

    def send(self, source_id: str, code: str, severity: str, message: str) -> None:
        import requests
        try:
            requests.post(
                self.url,
                json={
                    "source_id": source_id, "code": code, "severity": severity,
                    "message": message, "at": datetime.now(timezone.utc).isoformat(),
                },
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            log.error("alert webhook unreachable (%s) — falling back to log: %s/%s %s",
                      exc, source_id, code, message)


class Monitor:
    def __init__(
        self,
        db_path: str | Path = "monitoring.db",
        sink: AlertSink | None = None,
        stale_after_seconds: int = 3 * 3600,
        alert_cooldown_seconds: int = 1800,
    ) -> None:
        self.db = sqlite3.connect(str(db_path), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.sink = sink or LogSink()
        self.stale_after = stale_after_seconds
        self.cooldown = alert_cooldown_seconds
        self._suppressed: dict[tuple[str, str], tuple[float, int]] = {}
        self._init_schema()

    def _init_schema(self) -> None:
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT, source_id TEXT, started_at TEXT, duration_ms INTEGER,
            records_acquired INTEGER, records_clean INTEGER, records_quarantined INTEGER,
            health TEXT, parser_rule TEXT, drift INTEGER, content_hash TEXT, error TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_runs_source ON runs(source_id, started_at DESC);

        CREATE TABLE IF NOT EXISTS heartbeats (
            source_id TEXT PRIMARY KEY, last_success REAL, last_health TEXT, consecutive_failures INTEGER
        );

        CREATE TABLE IF NOT EXISTS alerts (
            at REAL, source_id TEXT, code TEXT, severity TEXT, message TEXT, suppressed_count INTEGER
        );
        """)
        self.db.commit()

    # ---- recording -------------------------------------------------------

    def record_run(self, run: RunRecord) -> None:
        self.db.execute(
            "INSERT INTO runs VALUES (:run_id,:source_id,:started_at,:duration_ms,:records_acquired,"
            ":records_clean,:records_quarantined,:health,:parser_rule,:drift,:content_hash,:error)",
            {**asdict(run), "drift": int(run.drift)},
        )

        previous = self.db.execute(
            "SELECT last_health, consecutive_failures FROM heartbeats WHERE source_id = ?",
            (run.source_id,),
        ).fetchone()
        previous_health = previous["last_health"] if previous else None
        failures = (previous["consecutive_failures"] if previous else 0)
        failures = failures + 1 if run.health == Health.FAILED else 0

        self.db.execute(
            "INSERT INTO heartbeats VALUES (?,?,?,?) ON CONFLICT(source_id) DO UPDATE SET "
            "last_success=excluded.last_success, last_health=excluded.last_health, "
            "consecutive_failures=excluded.consecutive_failures",
            (run.source_id, time.time(), run.health, failures),
        )
        self.db.commit()

        self._on_transition(run, previous_health, failures)

    def _on_transition(self, run: RunRecord, previous: str | None, failures: int) -> None:
        if run.health == previous:
            if run.health == Health.FAILED and failures in (3, 10, 25):
                self.alert(run.source_id, "still_failing", "error",
                           f"still failing after {failures} consecutive runs")
            return

        if run.health == Health.DEGRADED:
            self.alert(
                run.source_id, "schema_drift", "warn",
                f"parser fell through to fallback rule {run.parser_rule!r}; "
                f"{run.records_acquired} records recovered. Primary selector needs updating.",
            )
        elif run.health == Health.FAILED:
            self.alert(run.source_id, "source_failed", "error",
                       f"no records extracted ({run.error or 'all selectors missed'}); serving last-good")
        elif run.health == Health.HEALTHY and previous in (Health.DEGRADED, Health.FAILED, None):
            if previous is not None:
                self.alert(run.source_id, "recovered", "info",
                           f"back to healthy on the primary parser after {previous}")

    # ---- alerting --------------------------------------------------------

    def alert(self, source_id: str, code: str, severity: str, message: str) -> None:
        key = (source_id, code)
        now = time.time()
        last_sent, suppressed = self._suppressed.get(key, (0.0, 0))

        if now - last_sent < self.cooldown:
            self._suppressed[key] = (last_sent, suppressed + 1)
            log.debug("alert suppressed (%s/%s), %d within cooldown", source_id, code, suppressed + 1)
            return

        if suppressed:
            message = f"{message} [{suppressed} similar suppressed in the last {self.cooldown // 60}m]"

        self.db.execute("INSERT INTO alerts VALUES (?,?,?,?,?,?)",
                        (now, source_id, code, severity, message, suppressed))
        self.db.commit()
        self._suppressed[key] = (now, 0)
        self.sink.send(source_id, code, severity, message)

    # ---- health reporting ------------------------------------------------

    def check_heartbeats(self) -> list[str]:
        """Call on a schedule independent of the pipeline itself. This is the
        check that catches a pipeline which stopped being triggered at all."""
        stale: list[str] = []
        now = time.time()
        for row in self.db.execute("SELECT source_id, last_success FROM heartbeats"):
            age = now - row["last_success"]
            if age > self.stale_after:
                stale.append(row["source_id"])
                self.alert(row["source_id"], "stale_heartbeat", "error",
                           f"no successful run in {age / 3600:.1f}h (threshold {self.stale_after / 3600:.0f}h)")
        return stale

    def status(self) -> dict[str, Any]:
        rows = self.db.execute("SELECT * FROM heartbeats").fetchall()
        now = time.time()
        sources = {
            r["source_id"]: {
                "health": Health.STALE if (now - r["last_success"]) > self.stale_after else r["last_health"],
                "last_success_age_s": round(now - r["last_success"]),
                "consecutive_failures": r["consecutive_failures"],
            }
            for r in rows
        }
        healthy = sum(1 for s in sources.values() if s["health"] == Health.HEALTHY)
        degraded = sum(1 for s in sources.values() if s["health"] == Health.DEGRADED)
        return {
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "sources": sources,
            "pipeline_health_pct": round((healthy + degraded * 0.6) / len(sources) * 100, 1) if sources else 0.0,
        }

    def __str__(self) -> str:
        return json.dumps(self.status(), indent=2)
