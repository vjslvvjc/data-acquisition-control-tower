# Data Acquisition Control Tower

A working illustration of how I'd approach the Senior Data Acquisition & Automation
Specialist role in Better Collective's Research & Insights team.

**Live demo:** _[deploy URL]_ · **Author:** Vojislav Vujić ·
[LinkedIn](https://www.linkedin.com/in/vojislavvujic/)

---

## What this is, and what it isn't

The hosted demo runs on **bundled fixtures**. There are no live network calls, no API
key in the browser, and nothing here depends on a third party being up while somebody
is looking at it. What *is* real in the browser: the DOM parsing, the selector-fallback
chain, the schema validation, the duplicate collapsing and the drift detection.

The live-network implementation lives in `backend/` — retry and backoff, per-host rate
limiting, robots.txt handling, LLM enrichment against a structured-output contract, and
alert dispatch. That code is the part worth reading. The dashboard is how you get to it.

I've said this here and on the page itself, because a demo that quietly implies it's
doing more than it is fails the exact quality this pipeline is built around.

---

## The problem it's built for

Extraction is the easy half. Anyone can pull a page once. The half that costs money is
month three, when a source ships a redesign, a selector silently returns zero nodes, and
the pipeline writes an honest-looking empty result into the warehouse. Nobody sees an
error because nothing errored. The number is just quietly wrong, and stays wrong until
someone downstream happens to notice a chart looks odd.

So the architecture treats **source volatility as the default assumption**, not the
exception:

| Signal | Meaning | Response |
|---|---|---|
| Primary selector matched | Healthy | Record run, move on |
| Fallback rung matched | **Degraded** | Alert. Data flows, contract changed, fix within days |
| Every rung missed | **Failed** | Alert. Serve last-good. Do **not** write nulls as zeroes |
| Batch parsed, >25% defective | **Failed** | Reject batch, retain last-good |
| Record count outside 3σ | Anomaly | Warn — usually the earliest signal of upstream change |
| No heartbeat in 3h | **Stale** | Alert. Catches the pipeline that stopped being scheduled |

The distinction that matters most is *degraded* versus *failed*. A fallback that recovers
data silently becomes the permanent parser, and six months later nobody knows which
selector is load-bearing. Recovery is alertable precisely because it succeeded.

---

## Architecture

```
                  ┌──────────────┐
   external ──────│ acquisition  │  retry · backoff · rate limit · robots.txt
   sources        │              │  → Envelope (body + provenance + content hash)
                  └──────┬───────┘
                         │
                  ┌──────▼───────┐
                  │   parser     │  ordered selector chain, fallback on miss
                  │              │  → ParseResult (records + health + rung used)
                  └──────┬───────┘
                         │
                  ┌──────▼───────┐
                  │   enrich     │  batched LLM call, structured-output contract
                  │              │  → category + audience + confidence + model version
                  └──────┬───────┘
                         │
                  ┌──────▼───────┐
                  │  validator   │  schema · duplicates · rolling count baseline
                  │              │  → clean / quarantined split, never a silent drop
                  └──────┬───────┘
                         │
                  ┌──────▼───────┐
                  │  monitoring  │  heartbeats · health state machine · dedup'd alerts
                  │              │  → SQLite run history + webhook fan-out
                  └──────────────┘
```

```
backend/
  acquisition.py   Envelope, RateLimiter (token bucket, per host), RobotsCache, Acquirer
  parser.py        SelectorRule chains, FieldRule extraction, drift signalling
  enrich.py        Batched classification, taxonomy enforcement, confidence gating
  validator.py     FieldSpec schema, dedupe, CountBaseline (3σ on a rolling window)
  monitoring.py    Monitor, RunRecord, health transitions, alert cooldown, sinks
  pipeline.py      Orchestrator — per-source isolation, thread pool, partial success
samples/           Synthetic fixtures, including a "redesigned" page to trigger drift
index.html         The console
```

### Choices worth defending

**Provenance travels with the payload, not alongside it.** `Envelope` carries source, URL,
retrieval timestamp, content hash and attempt count. If a record reaches the warehouse
without those, we've already lost the ability to answer "where did this number come from"
three weeks later — which is the question that actually gets asked.

**Content hashing.** An unchanged hash across runs is information: either the source is
genuinely static, or we're being served a cache and thinking we're current.

**Enrichment degrades, never fails the run.** Losing a classification is an inconvenience.
Losing the acquisition is not. Enrichment is also batched, because per-record model calls
are how a €30/month pipeline becomes €3,000/month without anyone noticing.

**Model output is contract-checked.** The model returns JSON matching a declared schema
with a category from a closed taxonomy, or the batch is retried and then rejected. No
regex over prose. Every enriched field carries model version and confidence, so a bad
prompt revision can be rolled back without re-deriving everything.

**Alerts are deduplicated with a cooldown, and steady states aren't alertable.** A source
broken for a week shouldn't page anyone every fifteen minutes. Alert fatigue is how real
alerts get ignored. Repeated failures escalate at run 3, 10 and 25 instead.

**Sources are isolated in the orchestrator.** One source failing reports a partial success
honestly rather than throwing an exception that hides which four of the five were fine.

---

## Extraction ethics

Competitive intelligence depends on continued access. Getting blocked, rate-limited or
sent a letter is not a compliance abstraction — it's an outage.

- `robots.txt` is checked and honoured by default. Bypass requires an explicit
  per-source flag and is logged as a warning every time it's used.
- Per-host token-bucket rate limiting is on by default, with `Retry-After` honoured
  when a server sends one.
- Identifying user agent with a contact address, so an operator with a problem can
  reach a human instead of just blocking a subnet.
- Exponential backoff with jitter, so a shared upstream doesn't get a synchronised
  stampede from every worker at once.
- APIs are preferred over scraping wherever one exists, including paid ones. Cheaper
  than maintaining a parser against a hostile redesign schedule.

**The fixtures in this repository are synthetic.** No third-party site was scraped to
build this demo, and the operator names are invented. Using a job application as an
excuse to scrape real competitors seemed like the wrong opening move.

---

## Running it

```bash
pip install -r requirements.txt
export ANTHROPIC_API_KEY=...                    # omit and use --no-enrich
python -m backend.pipeline --db monitoring.db --webhook https://n8n.example/webhook/acq
python -m backend.pipeline --no-enrich -v       # verbose, no model calls
```

Open `index.html` directly in a browser for the console — no build step, no dependencies.

The n8n flow in `automation/` handles scheduling and alert fan-out. Keeping routing in
the automation platform rather than in this codebase means changing who gets told doesn't
need a deploy.

---

## From prototype to team

The posting is explicit that this role owns the pipelines first and then defines the
roadmap and hires against it. So here's the roadmap I'd bring on day one, to be torn
apart on contact with the actual source inventory.

**Days 0–30 — map and measure.** Inventory every source currently feeding Research &
Insights, including the manual ones nobody has written down. Classify by acquisition
pattern: documented API, undocumented API, static HTML, rendered HTML, file drop. Baseline
the cost of each manual workflow in hours per week — that becomes the prioritisation
ranking, and it's the number that justifies the second hire. Agree the monitoring standard
*before* building anything: what a heartbeat is, what counts as a break, who gets told.

**Days 30–60 — productionise the top of the list.** Ship the highest-value pipelines
against a shared extraction framework rather than one-off scripts. Provenance on every
record. Alerting live: heartbeat gaps, count anomalies, schema drift, latency regression.
Agree the handoff contract with the database owners so ingestion stops being a
negotiation every time.

**Days 60–90 — make it a team, not a person.** Pipeline catalogue with named ownership and
an SLA per source tier. Runbooks for the five failures that will actually happen, written
for someone who hasn't seen them before. Backlog scored by value and fragility, visible to
the commercial teams who consume it. Hiring profile for Extraction & Automation Specialist
#1, scoped against the gaps the catalogue exposes rather than against a generic job
description.

---

## Where I'd be learning

Straight answer, since it's more useful than a padded skills list.

Large-scale distributed scraping — headless browser fleets, proxy rotation, anti-bot
evasion at volume — is adjacent to what I've built rather than identical to it. So is the
paid media landscape specifically: tracking links, redirect chains, ad libraries. The
extraction, parsing, orchestration and reliability foundation transfers directly, which is
what this repository is meant to show. The domain specifics I'd expect to pick up in weeks.

---

MIT licensed. Built August 2026.
