"""
Orchestrator.

One rule shapes this module: a source failing must not take the run down.
Sources are isolated, each gets its own try boundary, and the run reports a
partial success honestly rather than an exception that hides which four of the
five sources were actually fine.

Run:  python -m backend.pipeline --config sources.yaml
"""

from __future__ import annotations

import argparse
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Sequence

from .acquisition import Acquirer, Envelope
from .enrich import Enricher
from .monitoring import Health, Monitor, RunRecord
from .parser import ParseHealth, SelectorRule, parse_html, parse_json
from .validator import CountBaseline, FieldSpec, Validator

log = logging.getLogger(__name__)


@dataclass
class SourceConfig:
    source_id: str
    url: str
    kind: str                                  # "html" | "json"
    chain: Sequence[SelectorRule] | None = None
    json_root: str | None = None
    json_mapping: dict[str, str] | None = None
    ignore_robots: bool = False


SCHEMA = [
    FieldSpec("operator", required=True, kind=str),
    FieldSpec("offer", required=True, kind=str),
    FieldSpec("value", required=False, kind=int, validate=lambda v: 0 <= v <= 10_000),
    FieldSpec("market", required=True, kind=str, validate=lambda v: len(v) in (2, 3)),
]


class Pipeline:
    def __init__(
        self,
        sources: Sequence[SourceConfig],
        monitor: Monitor,
        enricher: Enricher | None = None,
        max_workers: int = 4,
    ) -> None:
        self.sources = list(sources)
        self.monitor = monitor
        self.enricher = enricher
        self.acquirer = Acquirer()
        self.validator = Validator(
            schema=SCHEMA,
            dedupe_on=["operator", "offer", "market"],
            baseline=CountBaseline(),
        )
        self.max_workers = max_workers

    def run(self) -> dict:
        run_id = f"run-{uuid.uuid4().hex[:8]}"
        started = time.monotonic()
        log.info("=== %s starting · %d sources ===", run_id, len(self.sources))

        collected: list[dict] = []
        outcomes: dict[str, str] = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {pool.submit(self._run_source, run_id, s): s for s in self.sources}
            for future in as_completed(futures):
                source = futures[future]
                try:
                    records, health = future.result()
                    collected.extend(records)
                    outcomes[source.source_id] = health
                except Exception as exc:  # noqa: BLE001 — isolation is the point
                    log.exception("%s: unhandled failure", source.source_id)
                    outcomes[source.source_id] = Health.FAILED
                    self.monitor.record_run(RunRecord(
                        run_id=run_id, source_id=source.source_id,
                        started_at=datetime.now(timezone.utc).isoformat(),
                        duration_ms=0, records_acquired=0, records_clean=0, records_quarantined=0,
                        health=Health.FAILED, parser_rule=None, drift=False,
                        content_hash=None, error=str(exc)[:300],
                    ))

        # Enrichment runs once across the whole run, not per source — fewer,
        # larger calls. Failure here degrades to unenriched, never fails the run.
        if self.enricher and collected:
            collected = self.enricher.enrich(collected)

        duration = int((time.monotonic() - started) * 1000)
        summary = {
            "run_id": run_id,
            "duration_ms": duration,
            "records": len(collected),
            "sources": outcomes,
            "health": self.monitor.status()["pipeline_health_pct"],
        }
        log.info("=== %s complete · %dms · %d records · %s ===",
                 run_id, duration, len(collected), outcomes)
        return {"summary": summary, "records": collected}

    def _run_source(self, run_id: str, source: SourceConfig) -> tuple[list[dict], str]:
        started_wall = datetime.now(timezone.utc).isoformat()
        started = time.monotonic()
        envelope: Envelope | None = None
        error: str | None = None

        try:
            envelope = self.acquirer.fetch(
                source.source_id, source.url, ignore_robots=source.ignore_robots
            )
            if source.kind == "json":
                result = parse_json(envelope, source.json_root or "", source.json_mapping or {})
            else:
                result = parse_html(envelope, source.chain or [])
        except Exception as exc:  # noqa: BLE001
            error = str(exc)[:300]
            log.error("%s: acquisition/parse failed — %s", source.source_id, error)
            self.monitor.record_run(RunRecord(
                run_id=run_id, source_id=source.source_id, started_at=started_wall,
                duration_ms=int((time.monotonic() - started) * 1000),
                records_acquired=0, records_clean=0, records_quarantined=0,
                health=Health.FAILED, parser_rule=None, drift=False,
                content_hash=envelope.content_hash if envelope else None, error=error,
            ))
            return [], Health.FAILED

        report = self.validator.run(source.source_id, result.records)

        health = {
            ParseHealth.HEALTHY: Health.HEALTHY,
            ParseHealth.DEGRADED: Health.DEGRADED,
            ParseHealth.FAILED: Health.FAILED,
        }[result.health]
        if report.batch_rejected:
            health = Health.FAILED

        self.monitor.record_run(RunRecord(
            run_id=run_id, source_id=source.source_id, started_at=started_wall,
            duration_ms=int((time.monotonic() - started) * 1000),
            records_acquired=len(result.records),
            records_clean=len(report.clean),
            records_quarantined=len(report.quarantined),
            health=health, parser_rule=result.rule_used, drift=result.drifted,
            content_hash=envelope.content_hash,
            error="; ".join(i.message for i in report.issues if i.severity == "error") or None,
        ))

        return ([] if report.batch_rejected else report.clean), health


def main() -> None:
    ap = argparse.ArgumentParser(description="Run the acquisition pipeline once.")
    ap.add_argument("--db", default="monitoring.db")
    ap.add_argument("--webhook", help="n8n/Make webhook URL for alert fan-out")
    ap.add_argument("--no-enrich", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)-22s %(message)s",
    )

    sink = None
    if args.webhook:
        from .monitoring import WebhookSink
        sink = WebhookSink(args.webhook)

    monitor = Monitor(db_path=args.db, sink=sink)
    enricher = None if args.no_enrich else Enricher()

    from .sources import SOURCES  # site-specific config, kept out of the engine
    Pipeline(SOURCES, monitor, enricher).run()

    monitor.check_heartbeats()
    print(monitor)


if __name__ == "__main__":
    main()
