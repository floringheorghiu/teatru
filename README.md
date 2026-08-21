# teatru — staged harvester for Bucharest theatre programmes

Collects performance schedules from 12 Bucharest theatres/opera/circ venues,
extracts events with an LLM, and **mechanically verifies every extracted row
against the raw page snapshot** before publishing a spreadsheet.

The design principle: a model cannot be asked to promise it followed a
procedure, so every rule that *can* be mechanical *is* mechanical. The LLM
runs in exactly one stage (extract); everything else is deterministic Python
and cannot be skipped, faked, or "claimed complete".

## Pipeline stages (`teatre.py`)

| Stage | What it does | Model? |
|---|---|---|
| `preflight` | Prove the network works *before* diagnosing anything (DNS/TLS/timeout failures are named explicitly, per institution) | no |
| `harvest` | Fetch every calendar URL and snapshot the raw HTML to `snapshots/<period>/` — the evidence for everything downstream | no |
| `discover` | Find calendar URLs straight from a venue's homepage and merge them into `surse.json` | no |
| `diagnose` | Report what each snapshot actually contains (dates? event names? ticket links?) | no |
| `extract` | One model call per page, strict JSON out; deterministic parsers (json-ld / data attributes / inline JSON / fullcalendar) are tried first | **yes** |
| `verify` | Every row's quoted source string must literally occur in the snapshot, else the row is dropped; dates outside the period are dropped; invented links are replaced; exact duplicates collapsed | no |
| `render` | Emit the final event list as `.xlsx` (or `.csv` if `openpyxl` is missing), with a per-institution verification status sheet | no |

## Requirements

- Python 3 (stdlib only for the core pipeline)
- `curl` on PATH
- `pip install openpyxl` — for `.xlsx` output (falls back to `.csv` without it)
- `pip install playwright && playwright install chromium` — only needed for
  venues with `"strategy": "browser"` in `surse.json` (currently just TNB,
  whose calendar renders client-side)

Backend-specific:

| `--backend` | Needs |
|---|---|
| `none` | nothing — writes extraction prompts only |
| `ollama` | local Ollama (`OLLAMA_HOST`, default `http://127.0.0.1:11434`) |
| `cerebras` | `CEREBRAS_API_KEY` |
| `mimo` | `OPENROUTER_API_KEY` (MiMo-V2.5 via OpenRouter) |

Default models per backend:

| Backend | `teatre.py` | `teatre_graph.py` |
|---|---|---|
| `ollama` | `qwen3-coder:latest` | `gemma3:27b` |
| `cerebras` | `gpt-oss-120b` | `gpt-oss-120b` |
| `mimo` | `xiaomi/mimo-v2.5` | `xiaomi/mimo-v2.5` |

## Usage

```bash
# full pipeline for the current month
python3 teatre.py all --period current-month --backend ollama

# individual stages
python3 teatre.py preflight
python3 teatre.py harvest  --period 2026-09
python3 teatre.py diagnose --period 2026-09
python3 teatre.py extract  --period 2026-09 --backend cerebras --model gpt-oss-120b
python3 teatre.py verify   --period 2026-09
python3 teatre.py render   --period 2026-09
```

### Periods

```
current-month          the calendar month containing today
next-month
2026-09                a named month
2026-09-12             a single day
2026-09-12..2026-09-20 a range
```

Sites whose calendar takes a month parameter (e.g. ONB: `?luna=&anul=`) are
fetched once per month in the range; the rest are fetched once and filtered
by date.

### Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `TEATRE_BACKEND` | `none` | default LLM backend for `extract` |
| `TEATRE_MODEL` | per-backend default (see table above) | e.g. `gpt-oss-120b`, `qwen3-coder:latest`, `xiaomi/mimo-v2.5` |
| `TEATRE_CONCURRENCY` | `4` | parallel workers for network stages |
| `TEATRE_EXTRACT_CONCURRENCY` | `2` | parallel model calls — keep low; API tiers throttle tokens/minute, not requests |
| `TEATRE_MAX_TOKENS` | backend-specific | completion budget for model calls |
| `TEATRE_MAX_RETRIES` | `4` | retries with backoff on transient (429/5xx) model-call errors — OpenRouter free-tier rate limits |

## Output

- `rezultate/<period>.xlsx` — sheet **Spectacole**: Data, Ora, Instituția,
  Spectacol, Sala, Link de verificare; sheet **Stare verificare**: per-venue
  status (how many shows verified, or *why* there are none — not published
  yet, needs browser, extract failed, TLS unverified, …).
  Falls back to `rezultate/<period>.csv` (events only, UTF-8 with BOM for
  Excel) when `openpyxl` is not installed.
- `rezultate/<period>.verified.json` — verified and dropped rows with the
  exact reason each row was dropped.
- `snapshots/<period>/…` — raw HTML + metadata per fetch. The evidence trail;
  keep it if you want to re-run `verify`/`render` without re-harvesting.

`rezultate/`, `snapshots/`, `rezultate_graph/`, `snapshots_graph/`, and
`teatre_graph_checkpoints.db` are gitignored — they are regenerated outputs.

## Configuration: `surse.json`

Hand-edited registry of institutions: calendar URL, alt URLs, fetch strategy
(`html`/`browser`), whether the URL takes a month parameter, preferred
deterministic extractor, ticket-link domain policy, and dated discovery
notes explaining *why* each URL is what it is. Re-run `discover` quarterly or
whenever a venue's fetch starts failing.

Venues can be disabled with a `disabled_reason` (e.g. Masca — calendar too
hard to parse); they still appear in the status table as excluded.

## LangGraph variant: `teatre_graph.py`

The same pipeline expressed as a [LangGraph](https://langchain-ai.github.io/langgraph/)
graph — deterministic nodes with one model node inside. Adds:

- **checkpointing** (`teatre_graph_checkpoints.db`, SQLite): a crashed run
  resumes at the failed step with `--resume` instead of starting over
- typed state, map-reduce fan-out, conditional routing (abort on systemic
  failure vs. degrade on one dead venue is visible topology)
- **output isolation**: writes to `snapshots_graph/` and `rezultate_graph/`
  so it can never clobber a `teatre.py` run

```bash
pip install langgraph langgraph-checkpoint-sqlite

python3 teatre_graph.py --draw                          # print the topology (Mermaid)
python3 teatre_graph.py --period 2026-09 --backend none # dry run, no model
python3 teatre_graph.py --period 2026-09 --backend ollama
python3 teatre_graph.py --period 2026-09 --resume       # continue a crashed run
```

`teatre_graph_viz.html` (checked in) is a static rendering of the graph
topology — open it in a browser to see nodes/edges at a glance.
