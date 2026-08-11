"""
Parsing layer.

The assumption behind this module: any selector you write today will stop
matching, and you will not be told. So a parser is not one selector, it's an
ordered chain, and a fall through the chain is an event worth waking someone up
about — even when it succeeds.

The distinction that matters operationally:

    primary matched            -> healthy
    fallback matched           -> DEGRADED. Data is flowing, the contract has
                                  changed, someone has ~days to fix it properly.
    nothing matched            -> FAILED. Serve last-good, do not write nulls
                                  into the warehouse and pretend they're zeroes.

Silent recovery is the failure mode this is built to prevent. A fallback that
nobody hears about becomes the permanent parser, and six months later nobody
knows which selector is load-bearing.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Sequence

from bs4 import BeautifulSoup, Tag

from .acquisition import Envelope

log = logging.getLogger(__name__)


class ParseHealth(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"


@dataclass(frozen=True)
class FieldRule:
    """How to pull one field out of an item node."""

    selector: str | None = None
    attr: str | None = None          # read this attribute instead of text
    on_item: bool = False            # attribute lives on the item node itself
    cast: Callable[[str], object] = str

    def extract(self, item: Tag) -> object | None:
        node = item if self.on_item else (item.select_one(self.selector) if self.selector else None)
        if node is None:
            return None
        raw = node.get(self.attr) if self.attr else node.get_text(strip=True)
        if raw is None or raw == "":
            return None
        try:
            return self.cast(raw)
        except (TypeError, ValueError):
            log.debug("cast failed for %r via %s", raw, self.cast)
            return None


@dataclass(frozen=True)
class SelectorRule:
    """One rung of the fallback chain."""

    label: str
    item_selector: str
    fields: dict[str, FieldRule]


@dataclass
class ParseResult:
    records: list[dict] = field(default_factory=list)
    health: ParseHealth = ParseHealth.FAILED
    rule_used: str | None = None
    chain_position: int = -1
    chain_length: int = 0
    parser_version: str = "0.3"

    @property
    def drifted(self) -> bool:
        return self.chain_position > 0


def parse_html(envelope: Envelope, chain: Sequence[SelectorRule]) -> ParseResult:
    """Walk the chain until a rung yields nodes. Report where we landed."""
    soup = BeautifulSoup(envelope.body, "html.parser")

    for position, rule in enumerate(chain):
        nodes = soup.select(rule.item_selector)
        if not nodes:
            log.debug("%s: selector %r matched 0 nodes", envelope.source_id, rule.item_selector)
            continue

        records = []
        for node in nodes:
            record = {name: frule.extract(node) for name, frule in rule.fields.items()}
            record["_provenance"] = {
                "source_id": envelope.source_id,
                "url": envelope.url,
                "retrieved_at": envelope.retrieved_at,
                "content_hash": envelope.content_hash,
                "parser_rule": rule.label,
                "parser_version": "0.3",
            }
            records.append(record)

        health = ParseHealth.HEALTHY if position == 0 else ParseHealth.DEGRADED
        if position > 0:
            log.warning(
                "%s: SCHEMA DRIFT — primary selector %r missed, recovered on rung %d/%d (%s), %d records",
                envelope.source_id, chain[0].item_selector, position + 1, len(chain), rule.label, len(records),
            )

        return ParseResult(
            records=records,
            health=health,
            rule_used=rule.label,
            chain_position=position,
            chain_length=len(chain),
        )

    log.error(
        "%s: every rung of the selector chain missed (%d tried). Marking FAILED — last-good snapshot should be served.",
        envelope.source_id, len(chain),
    )
    return ParseResult(health=ParseHealth.FAILED, chain_length=len(chain))


def parse_json(envelope: Envelope, root: str, mapping: dict[str, str]) -> ParseResult:
    """
    Flat mapping for JSON feeds. `root` is a dotted path to the record array,
    `mapping` is target_field -> source_field.

    JSON gets the same treatment as HTML: a missing root is drift, not an
    exception to swallow. APIs rename fields between minor versions constantly.
    """
    import json

    payload = json.loads(envelope.body)
    node: object = payload
    for part in root.split("."):
        if not isinstance(node, dict) or part not in node:
            log.error("%s: root path %r broke at %r — treating as drift", envelope.source_id, root, part)
            return ParseResult(health=ParseHealth.FAILED)
        node = node[part]

    if not isinstance(node, list):
        log.error("%s: root path %r did not resolve to a list", envelope.source_id, root)
        return ParseResult(health=ParseHealth.FAILED)

    missing_keys: set[str] = set()
    records = []
    for item in node:
        record = {}
        for target, source in mapping.items():
            if source not in item:
                missing_keys.add(source)
            record[target] = item.get(source)
        record["_provenance"] = {
            "source_id": envelope.source_id,
            "url": envelope.url,
            "retrieved_at": envelope.retrieved_at,
            "content_hash": envelope.content_hash,
            "parser_rule": f"json:{root}",
            "parser_version": "0.3",
        }
        records.append(record)

    health = ParseHealth.HEALTHY
    if missing_keys:
        health = ParseHealth.DEGRADED
        log.warning(
            "%s: expected keys absent from payload: %s — feed contract may have changed",
            envelope.source_id, ", ".join(sorted(missing_keys)),
        )

    return ParseResult(
        records=records,
        health=health,
        rule_used=f"json:{root}",
        chain_position=0,
        chain_length=1,
    )
