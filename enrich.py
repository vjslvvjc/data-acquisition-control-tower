"""
AI enrichment layer.

Turning unstructured competitive signal into structured fields is the part of
this pipeline where a model earns its place. It is also the part most likely to
quietly poison the warehouse, so the rules here are stricter than elsewhere:

  - The model returns JSON matching a declared contract, or the batch is
    rejected. No free-text parsing, no regex over prose.
  - Every enriched field carries a confidence and the model version that
    produced it, so a bad prompt revision can be identified and rolled back
    without re-deriving everything.
  - Enrichment failure degrades to unenriched records. It never fails the run.
    Losing a classification is an inconvenience; losing the acquisition is not.
  - Batched, not per-record. Per-record calls are how a €30/month pipeline
    becomes a €3,000/month pipeline without anyone noticing.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Sequence

import anthropic

log = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-6"
BATCH_SIZE = 25

TAXONOMY = [
    "acquisition:free_bet",
    "acquisition:deposit_match",
    "acquisition:no_deposit",
    "retention:loss_protection",
    "retention:engagement",
    "retention:loyalty",
    "unclassified",
]

SYSTEM = f"""You classify sports betting promotional offers for a competitive intelligence pipeline.

Return ONLY a JSON array. No prose, no markdown fences, no explanation.

Each element must be exactly:
{{"id": <int, the id given in the input>,
  "category": <one of {json.dumps(TAXONOMY)}>,
  "audience": "new" | "existing" | "both",
  "confidence": <float 0.0-1.0>}}

Rules:
- Return exactly one element per input element, preserving ids.
- If an offer is ambiguous or the text is truncated, use "unclassified" with
  confidence below 0.5 rather than guessing a plausible-looking category.
- Confidence reflects your certainty about the category, not how attractive the
  offer is."""


@dataclass(frozen=True)
class Enrichment:
    category: str
    audience: str
    confidence: float
    model: str
    enriched_at: float

    def as_fields(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "audience": self.audience,
            "classification_confidence": round(self.confidence, 3),
            "_enrichment": {"model": self.model, "at": self.enriched_at},
        }


class Enricher:
    def __init__(
        self,
        api_key: str | None = None,
        model: str = MODEL,
        min_confidence: float = 0.55,
        max_retries: int = 2,
    ) -> None:
        key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            raise RuntimeError("ANTHROPIC_API_KEY not set")
        self.client = anthropic.Anthropic(api_key=key)
        self.model = model
        self.min_confidence = min_confidence
        self.max_retries = max_retries

    def enrich(self, records: Sequence[dict]) -> list[dict]:
        """
        Returns records with enrichment fields merged in. Records the model
        couldn't classify confidently come back flagged for review rather than
        silently mislabelled.
        """
        out: list[dict] = []
        for start in range(0, len(records), BATCH_SIZE):
            batch = list(records[start:start + BATCH_SIZE])
            try:
                results = self._classify_batch(batch)
            except Exception as exc:  # noqa: BLE001 — degrade, never fail the run
                log.error("enrichment failed for batch at offset %d (%s) — passing through unenriched",
                          start, exc)
                out.extend({**r, "category": None, "_enrichment_error": str(exc)[:200]} for r in batch)
                continue

            for index, record in enumerate(batch):
                enrichment = results.get(index)
                if enrichment is None:
                    out.append({**record, "category": None, "_enrichment_error": "no result returned for id"})
                    continue
                if enrichment.confidence < self.min_confidence:
                    log.info("low-confidence classification (%.2f) — flagged for review",
                             enrichment.confidence)
                    out.append({**record, **enrichment.as_fields(), "_review_required": True})
                    continue
                out.append({**record, **enrichment.as_fields()})
        return out

    def _classify_batch(self, batch: Sequence[dict]) -> dict[int, Enrichment]:
        payload = [
            {
                "id": index,
                "operator": record.get("operator"),
                "offer": record.get("offer"),
                "value": record.get("value"),
                "market": record.get("market"),
            }
            for index, record in enumerate(batch)
        ]

        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 2):
            try:
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=2000,
                    system=SYSTEM,
                    messages=[{"role": "user", "content": json.dumps(payload, ensure_ascii=False)}],
                )
                text = "".join(b.text for b in response.content if b.type == "text").strip()
                return self._parse_response(text, expected=len(batch))
            except (json.JSONDecodeError, ValueError) as exc:
                last_error = exc
                log.warning("enrichment attempt %d returned unparseable output (%s)", attempt, exc)
                time.sleep(2 ** attempt)
            except anthropic.APIError as exc:
                last_error = exc
                log.warning("enrichment attempt %d hit API error (%s)", attempt, exc)
                time.sleep(2 ** attempt)

        raise RuntimeError(f"enrichment failed after {self.max_retries + 1} attempts") from last_error

    def _parse_response(self, text: str, expected: int) -> dict[int, Enrichment]:
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        parsed = json.loads(text)

        if not isinstance(parsed, list):
            raise ValueError(f"expected a JSON array, got {type(parsed).__name__}")

        now = time.time()
        results: dict[int, Enrichment] = {}
        for element in parsed:
            if not isinstance(element, dict) or "id" not in element:
                raise ValueError("array element missing 'id'")
            category = element.get("category")
            if category not in TAXONOMY:
                raise ValueError(f"category {category!r} outside declared taxonomy")
            results[int(element["id"])] = Enrichment(
                category=category,
                audience=element.get("audience", "both"),
                confidence=float(element.get("confidence", 0.0)),
                model=self.model,
                enriched_at=now,
            )

        if len(results) != expected:
            log.warning("enrichment returned %d results for %d records — missing ids flagged",
                        len(results), expected)
        return results
