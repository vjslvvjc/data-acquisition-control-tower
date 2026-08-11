"""
Validation layer.

Three classes of problem, deliberately kept separate because they need
different responses:

  1. Record-level defects  -> quarantine the record, keep the batch.
  2. Batch-level anomalies -> the batch parsed fine but doesn't look like the
                              batches before it. Usually the earliest signal
                              that something upstream changed.
  3. Contract violations   -> refuse the batch entirely.

Quarantine rather than drop. A dropped record is invisible; a quarantined one
is a queue somebody can look at, and the size of that queue is a metric.
"""

from __future__ import annotations

import logging
import statistics
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Iterable, Sequence

log = logging.getLogger(__name__)


class Severity(str, Enum):
    INFO = "info"
    WARN = "warn"
    ERROR = "error"


@dataclass(frozen=True)
class Issue:
    severity: Severity
    code: str
    message: str
    record_index: int | None = None


@dataclass(frozen=True)
class FieldSpec:
    name: str
    required: bool = True
    kind: type = str
    validate: Callable[[Any], bool] | None = None


@dataclass
class ValidationReport:
    clean: list[dict] = field(default_factory=list)
    quarantined: list[dict] = field(default_factory=list)
    issues: list[Issue] = field(default_factory=list)
    duplicates_collapsed: int = 0
    batch_rejected: bool = False

    @property
    def ok(self) -> bool:
        return not self.batch_rejected and not any(i.severity is Severity.ERROR for i in self.issues)

    def summary(self) -> str:
        return (
            f"{len(self.clean)} clean, {len(self.quarantined)} quarantined, "
            f"{self.duplicates_collapsed} duplicates, {len(self.issues)} issues"
        )


class CountBaseline:
    """
    Rolling record-count baseline per source.

    A fixed threshold is wrong for sources that legitimately vary. This keeps a
    window and flags on standard deviations, falling back to a percentage rule
    until there's enough history to have an opinion.
    """

    def __init__(self, window: int = 20, sigma: float = 3.0, min_history: int = 5) -> None:
        self._history: dict[str, deque[int]] = {}
        self.window = window
        self.sigma = sigma
        self.min_history = min_history

    def observe(self, source_id: str, count: int) -> None:
        self._history.setdefault(source_id, deque(maxlen=self.window)).append(count)

    def check(self, source_id: str, count: int) -> Issue | None:
        history = self._history.get(source_id)
        if not history:
            return None

        if len(history) < self.min_history:
            mean = statistics.fmean(history)
            if mean and abs(count - mean) / mean >= 0.30:
                return Issue(
                    Severity.WARN, "count_anomaly",
                    f"{source_id}: {count} records vs mean {mean:.0f} over {len(history)} runs "
                    f"({(count - mean) / mean:+.0%}) — provisional 30% rule, history still short",
                )
            return None

        mean = statistics.fmean(history)
        stdev = statistics.pstdev(history)
        if stdev == 0:
            if count != mean:
                return Issue(
                    Severity.WARN, "count_anomaly",
                    f"{source_id}: {count} records — source has returned exactly {mean:.0f} "
                    f"for {len(history)} runs, any change is notable",
                )
            return None

        z = abs(count - mean) / stdev
        if z >= self.sigma:
            return Issue(
                Severity.WARN, "count_anomaly",
                f"{source_id}: {count} records is {z:.1f}σ from mean {mean:.0f} (σ={stdev:.1f})",
            )
        return None


class Validator:
    def __init__(
        self,
        schema: Sequence[FieldSpec],
        dedupe_on: Sequence[str] | None = None,
        baseline: CountBaseline | None = None,
        max_quarantine_ratio: float = 0.25,
    ) -> None:
        self.schema = list(schema)
        self.dedupe_on = list(dedupe_on or [])
        self.baseline = baseline or CountBaseline()
        self.max_quarantine_ratio = max_quarantine_ratio

    def run(self, source_id: str, records: Iterable[dict]) -> ValidationReport:
        records = list(records)
        report = ValidationReport()

        # ---- 1. record level -------------------------------------------------
        for index, record in enumerate(records):
            problems = self._check_record(record, index)
            if problems:
                report.issues.extend(problems)
                report.quarantined.append({**record, "_quarantine_reason": [p.code for p in problems]})
            else:
                report.clean.append(record)

        # ---- 2. duplicates ---------------------------------------------------
        if self.dedupe_on:
            seen: set[tuple] = set()
            deduped = []
            for record in report.clean:
                key = tuple(record.get(f) for f in self.dedupe_on)
                if key in seen:
                    report.duplicates_collapsed += 1
                    continue
                seen.add(key)
                deduped.append(record)
            report.clean = deduped
            if report.duplicates_collapsed:
                report.issues.append(Issue(
                    Severity.WARN, "duplicates",
                    f"{source_id}: {report.duplicates_collapsed} duplicate(s) collapsed on "
                    f"({', '.join(self.dedupe_on)})",
                ))

        # ---- 3. batch level --------------------------------------------------
        anomaly = self.baseline.check(source_id, len(records))
        if anomaly:
            report.issues.append(anomaly)
        self.baseline.observe(source_id, len(records))

        if records:
            ratio = len(report.quarantined) / len(records)
            if ratio > self.max_quarantine_ratio:
                report.batch_rejected = True
                report.issues.append(Issue(
                    Severity.ERROR, "batch_rejected",
                    f"{source_id}: {ratio:.0%} of records failed schema (ceiling "
                    f"{self.max_quarantine_ratio:.0%}) — batch rejected, last-good retained",
                ))
                log.error("%s: batch rejected, %d/%d records defective", source_id,
                          len(report.quarantined), len(records))
        elif not records:
            report.batch_rejected = True
            report.issues.append(Issue(
                Severity.ERROR, "empty_batch",
                f"{source_id}: parser returned zero records — treated as failure, not as an empty day",
            ))

        log.info("%s: validation — %s", source_id, report.summary())
        return report

    def _check_record(self, record: dict, index: int) -> list[Issue]:
        issues: list[Issue] = []
        for spec in self.schema:
            value = record.get(spec.name)

            if value is None or value == "":
                if spec.required:
                    issues.append(Issue(
                        Severity.WARN, "missing_field",
                        f"record {index}: required field {spec.name!r} is empty", index,
                    ))
                continue

            if not isinstance(value, spec.kind):
                issues.append(Issue(
                    Severity.WARN, "type_mismatch",
                    f"record {index}: {spec.name!r} is {type(value).__name__}, expected {spec.kind.__name__}",
                    index,
                ))
                continue

            if spec.validate and not spec.validate(value):
                issues.append(Issue(
                    Severity.WARN, "constraint_failed",
                    f"record {index}: {spec.name!r}={value!r} failed its constraint", index,
                ))
        return issues
