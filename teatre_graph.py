#!/usr/bin/env python3
"""
teatre_graph.py — the teatre.py pipeline expressed as a LangGraph graph.

WHY THIS EXISTS
    Not because the pipeline needed agents. It didn't — and the central lesson
    of building teatre.py was the opposite: every rule that CAN be mechanical
    should be, because a model cannot be asked to promise it followed a
    procedure.

    What LangGraph adds is not autonomy. It is orchestration infrastructure you
    would otherwise hand-roll: typed state that flows between steps, map-reduce
    fan-out, checkpointing so a crashed run resumes at the failed step instead
    of from zero, conditional routing, and a trace of every node's input/output
    for free.

CURRENT WITH teatre.py
    This graph delegates the heavy lifting to teatre.py's own stage functions,
    so it inherits every fix automatically:

      * harvest   -> T.harvest(): threaded curl fan-out, ONE shared Chromium
                     for browser-strategy venues, snapshots + condensed text
                     + meta.json + the CANARY check (dates lost in condensing
                     are flagged, never silently swallowed).
      * diagnose  -> T.diagnose(): full per-snapshot verdict table.
      * extract   -> per-snapshot Send() fan-out, but each worker runs
                     T.deterministic_rows() FIRST (json-ld / data-attrs /
                     inline-json / fullcalendar ladder, honoring each venue's
                     "extract" config in surse.json). Only pages the parsers
                     cannot handle reach a model, and model calls are capped
                     with a semaphore (--extract-concurrency, default 2)
                     because API tiers limit tokens/minute, not requests.
                     For external backends (cerebras, mimo), the prompt
                     text is PII-redacted: institutional emails and phone
                     numbers from venue pages are scrubbed before the API
                     call to prevent provider-side PII filters from blocking
                     the request.
                     For external backends (cerebras, mimo), the prompt
                     text is PII-redacted: institutional emails and phone
                     numbers from venue pages are scrubbed before the API
                     call to prevent provider-side PII filters from blocking
                     the request.
      * verify    -> T.verify(): quote-in-snapshot gate + period gate +
                     link-checked-against-full-HTML fallback + dedup.
      * render    -> T.render(): xlsx/csv + status table.

    Read the graph as: DETERMINISTIC NODES with ONE model node inside.
    That ratio is the design, not an accident.

        START
          │
          ▼
      preflight ───(systemic failure)──▶ abort ──▶ END
          │
          ▼
         plan                      ← expand institutions × months (report only)
          │
          ▼
        fetch                      ← T.harvest: threaded curl + ONE shared
          │                          Chromium + canary on every snapshot
          ▼
       diagnose                    ← T.diagnose: full verdict table
          │  Send(...) one per fetched snapshot
   ┌──────┼──────┐
   ▼      ▼      ▼
extract_one … extract_one          ← deterministic ladder first; THE ONLY
   └──────┬──────┘                    MODEL NODE is the fallback rung,
          ▼                           semaphore-capped
       verify                      ← T.verify. Pure function. Never a model.
          │
          ▼
        render                     ← T.render
          │
          ▼
         END

OUTPUT ISOLATION
    This script writes to its OWN directories so it never clobbers teatre.py:
        snapshots_graph/<period>/    (.html / .txt / .meta.json / .result.json)
        rezultate_graph/<period>.verified.json | .xlsx | .csv
        teatre_graph_checkpoints.db  (resume state)

Install:
    pip install langgraph langgraph-checkpoint-sqlite

Run:
    python3 teatre_graph.py --period current-month --backend none
    python3 teatre_graph.py --period 2026-09 --backend ollama
    python3 teatre_graph.py --period 2026-09 --resume     # continue a run
    python3 teatre_graph.py --draw                        # print the graph

Tested against langgraph 1.x / langgraph-checkpoint-sqlite on Python 3.14.
"""

from __future__ import annotations

import argparse
import json
import operator
import re
import sys
import threading
import time
from typing import Annotated, Any, TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

try:
    from langgraph.checkpoint.sqlite import SqliteSaver
except ImportError:  # sqlite checkpointer is optional; memory still works
    SqliteSaver = None

# The deterministic core is imported, not reimplemented. LangGraph supplies
# control flow; it does not replace a single line of the logic that makes the
# results trustworthy.
import teatre as T

# ---------------------------------------------------------------- isolation
# Redirect teatre.py's module-level directories BEFORE any stage runs, so
# every artifact this graph produces lands in *_graph locations and can never
# overwrite a teatre.py run.
T.SNAPDIR = T.ROOT / "snapshots_graph"
T.OUTDIR = T.ROOT / "rezultate_graph"
CHECKPOINT_DB = T.ROOT / "teatre_graph_checkpoints.db"

# Semaphore that caps true model-call parallelism across Send() workers.
# Send nodes run on threads, so a module-level threading.Semaphore works.
# Default 2 mirrors teatre.py: API tiers throttle tokens/minute, not requests.
EXTRACT_SEM: threading.Semaphore = threading.Semaphore(2)


# ---------------------------------------------------------------- PII guard
# Venue pages carry institutional emails/phones; condense() keeps any line near
# a date, so those tokens can ride along into the prompt. Some hosted APIs (and
# tool-output filters) reject or redact the request on sight. Schedule rows
# never need an email or a 9-10 digit phone number, so scrub them from the
# model payload. Verify still checks quotes against the unredacted snapshot
# text on disk; date quotes are unaffected by this scrub.
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?<!\d)(?:\+?40|0040|0)\d{2,3}[ .\-]?\d{3}[ .\-]?\d{3}(?:[ .\-]?\d)?(?!\d)")


def redact_pii(text: str) -> str:
    return PHONE_RE.sub("[REDACTED-PHONE]", EMAIL_RE.sub("[REDACTED-EMAIL]", text))


# ─────────────────────────────────────────────────────────── state
class GraphState(TypedDict, total=False):
    """The typed channel every node reads from and writes to.

    `Annotated[list, operator.add]` is the load-bearing detail. Nodes launched
    by Send() run CONCURRENTLY and all write to the same key. Without a
    reducer, LangGraph raises InvalidUpdateError("At key 'results': Can
    receive only one value per step") — because it cannot know whether two
    concurrent writes should overwrite or merge. `operator.add` says
    "concatenate the lists". This is the single most common thing people get
    wrong when they first use fan-out.
    """
    period: str
    backend: str
    model: str
    concurrency: int

    reachable: dict[str, str]
    aborted: bool
    abort_reason: str

    results: Annotated[list[dict[str, Any]], operator.add]   # fan-in
    timings: Annotated[list[tuple[str, float]], operator.add]
    n_verified: int
    n_dropped: int


class ExtractTask(TypedDict):
    """Payload handed to one extract_one worker.

    A node reached via Send receives THIS, not the whole GraphState. That is
    the point of Send: each worker gets only its slice, so workers stay
    independent and the framework can run them in parallel safely.
    """
    period: str
    stem: str
    slug: str
    backend: str
    model: str


# ─────────────────────────────────────────────────────────── nodes
def n_preflight(state: GraphState) -> dict:
    """Stage 0. Plain function. Returns a partial state dict — never mutate state."""
    t0 = time.monotonic()
    code, reachable = T.preflight(verbose=False)
    ok = sum(1 for v in reachable.values() if v == "OK")
    degraded = [k for k, v in reachable.items() if v != "OK"]
    print(f"[preflight] {ok}/{len(reachable)} reachable"
          + (f" — degraded: {', '.join(degraded)}" if degraded else ""))
    return {
        "reachable": reachable,
        "aborted": bool(code),
        "abort_reason": "no source reachable — network, not the sites" if code else "",
        "timings": [("preflight", time.monotonic() - t0)],
    }


def r_after_preflight(state: GraphState) -> str:
    """A conditional edge is just a function returning the NEXT NODE'S NAME.

    This is how a graph encodes 'abort on systemic failure, degrade on a single
    dead venue' — the same rule teatre.py enforces, now visible as topology.
    """
    return "abort" if state.get("aborted") else "plan"


def n_abort(state: GraphState) -> dict:
    print(f"[abort] {state.get('abort_reason')}")
    return {}


def n_plan(state: GraphState) -> dict:
    """Stage 1. Expand institutions × months into fetch units (report only).

    The actual fetching is delegated to T.harvest, which owns the
    curl-vs-browser split internally; this node exists so the plan is visible
    in the graph trace before any network traffic.
    """
    period = T.parse_period(state["period"])
    units = 0
    for inst in T.load_sources():                     # disabled venues excluded here
        parts = T.parts_for(inst, period)
        units += len(parts)
        print(f"  [plan] {inst['slug']:<12} {inst['strategy']:<8} "
              f"{len(parts)} part(s): {', '.join(p for p, _ in parts)}")
    print(f"[plan] {units} fetch unit(s) for {period.human()}")
    return {}


def n_fetch(state: GraphState) -> dict:
    """Stage 2 — T.harvest: threaded curl + ONE shared Chromium + canary.

    Delegation matters here: teatre.py's harvest runs curl parts in a thread
    pool, batches every browser-strategy page through a single Chromium
    instance (launch+teardown costs seconds each), writes .html/.txt/.meta.json
    per part, and runs the CANARY on every snapshot — flagging in-period dates
    that condensing would have lost. Reimplementing that per-Send would lose
    the shared browser and the canary report.
    """
    t0 = time.monotonic()
    period = T.parse_period(state["period"])
    T.harvest(period, concurrency=state.get("concurrency", 4))
    return {"timings": [("harvest", time.monotonic() - t0)]}


def n_diagnose(state: GraphState) -> dict:
    """Stage 2b — the full teatre.py diagnosis, not a stub.

    Reads every snapshot from disk and reports ld+json Event counts, Next.js
    payloads, date density, month mentions, and a verdict per venue, plus the
    out-of-season note when most snapshots never mention the target month.
    """
    t0 = time.monotonic()
    T.diagnose(T.parse_period(state["period"]))
    return {"timings": [("diagnose", time.monotonic() - t0)]}


def fan_out_extract(state: GraphState) -> list[Send] | str:
    """Map step over snapshot meta files on disk.

    One Send per fetched (or failed) snapshot. Returning a plain node name
    short-circuits the fan-out when there is nothing to extract.
    """
    period = T.parse_period(state["period"])
    day = T.SNAPDIR / period.label
    metas = sorted(day.glob("*.meta.json")) if day.exists() else []
    if not metas:
        return "verify"
    sends = []
    for mp in metas:
        meta = json.loads(mp.read_text(encoding="utf-8"))
        sends.append(Send("extract_one", ExtractTask(
            period=state["period"],
            stem=f"{meta['slug']}__{meta['part']}",
            slug=meta["slug"],
            backend=state.get("backend", "none"),
            model=state.get("model", ""))))
    print(f"[extract] fanning out {len(sends)} snapshot(s) "
          f"(backend={state.get('backend', 'none')})")
    return sends


def n_extract_one(task: ExtractTask) -> dict:
    """Stage 3 — deterministic ladder FIRST, model only as the fallback rung.

    Mirrors teatre.py exactly:
      1. If the venue publishes machine-readable timestamps (json-ld Events,
         data-fulldate attrs, inline "start" JSON, FullCalendar grids) and its
         surse.json "extract" config is not "llm", parse with
         T.deterministic_rows — free, instant, cannot hallucinate. NO model.
      2. Otherwise build the EXTRACT_PROMPT from the condensed text and call
         the backend — but only inside the EXTRACT_SEM semaphore, so at most
         --extract-concurrency model calls run at once (API tiers limit
         tokens/minute, not requests).
      3. backend == "none" writes the prompt to snapshots_graph/<period>/prompts/
         for manual paste, exactly like teatre.py.
    """
    period = T.parse_period(task["period"])
    day = T.SNAPDIR / period.label
    stem = task["stem"]
    t0 = time.monotonic()

    meta = json.loads((day / f"{stem}.meta.json").read_text(encoding="utf-8"))
    if meta["status"] != "FETCHED":
        T.write_result(day, stem, {"status": meta["status"], "rows": []})
        print(f"  [extract] {stem:<24} skipped ({meta['status']})")
        return {"results": [{"stem": stem, "status": meta["status"], "rows": []}],
                "timings": [(f"extract:{stem}", time.monotonic() - t0)]}

    raw_html = (day / f"{stem}.html").read_text(encoding="utf-8", errors="replace")

    # LADDER STEP 1 — deterministic (honors per-venue "extract" config).
    cfg = {i["slug"]: i for i in T.load_sources(include_disabled=True)}
    want = cfg.get(task["slug"], {}).get("extract", "auto")
    if want != "llm":
        strat, rows = T.deterministic_rows(raw_html, period, want)
        if rows:
            T.write_result(day, stem, {"status": "OK", "published_through": "",
                                       "method": strat, "rows": rows})
            dt_s = time.monotonic() - t0
            print(f"  [extract] {stem:<24} OK rows={len(rows):<3} "
                  f"via {strat} (no model) [{T.fmt_dur(dt_s)}]")
            return {"results": [{"stem": stem, "status": "OK",
                                 "method": strat, "rows": rows}],
                    "timings": [(f"extract:{stem}", dt_s)]}

    # LADDER STEP 2 — the model, on condensed prose the parsers can't read.
    tp = day / f"{stem}.txt"
    text = tp.read_text(encoding="utf-8") if tp.exists() else T.condense(raw_html, period)
    if task["backend"] in ("cerebras", "mimo"):
        text = redact_pii(text)
    prompt = T.EXTRACT_PROMPT.format(
        name=meta["name"], ts=meta["fetched_at"], url=meta["url"],
        start=period.start.isoformat(), end=period.end.isoformat(), html=text)
    pdir = day / "prompts"
    pdir.mkdir(parents=True, exist_ok=True)
    (pdir / f"{stem}.txt").write_text(prompt, encoding="utf-8")

    if task["backend"] == "none":
        print(f"  [extract] {stem:<24} prompt written ({len(prompt)} chars) "
              f"— no backend, skipping model call")
        return {"results": [{"stem": stem, "status": "PROMPT_WRITTEN", "rows": []}],
                "timings": [(f"extract:{stem}", time.monotonic() - t0)]}

    backends = {"ollama": T.call_ollama, "mimo": T.call_mimo,
                "cerebras": T.call_cerebras}
    # Semaphore keeps true model-call parallelism at --extract-concurrency even
    # though LangGraph may run many Send workers on threads at once.
    with EXTRACT_SEM:
        try:
            raw = backends[task["backend"]](prompt, task["model"])
            result = T.parse_json_loose(raw)
            result.setdefault("status", "OK")
            result.setdefault("rows", [])
        except Exception as exc:
            # One bad page must not kill the run. In a hand-rolled loop this is
            # a try/except; in a graph it is a per-node concern that never
            # propagates.
            result = {"status": "EXTRACT_FAILED", "rows": [], "error": str(exc)[:300]}

    T.write_result(day, stem, result)
    dt_s = time.monotonic() - t0
    print(f"  [extract] {stem:<24} {result['status']:<16} "
          f"rows={len(result.get('rows', [])):<3} "
          f"{result.get('error', '')} [{T.fmt_dur(dt_s)}]")
    return {"results": [{"stem": stem, **result}],
            "timings": [(f"extract:{stem}", dt_s)]}


def n_verify(state: GraphState) -> dict:
    """Stage 4. DELIBERATELY NOT AN AGENT, and the most important node here.

    Everything upstream is a proposal. This is the only place a row becomes a
    fact, and it must be a pure function of (row, snapshot) — auditable,
    reproducible, and impossible to talk out of. T.verify enforces both gates:
    citat_sursa must literally occur in the condensed evidence, and data must
    fall inside the period; links are checked against the FULL snapshot HTML
    and replaced with the source URL when invented. The moment you let a model
    decide whether a model's output is correct, you have a system that cannot
    be wrong on paper and is routinely wrong in practice.
    """
    t0 = time.monotonic()
    period = T.parse_period(state["period"])
    T.verify(period)                                   # same gates, same code path
    out = T.OUTDIR / f"{period.label}.verified.json"
    data = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
    return {"timings": [("verify", time.monotonic() - t0)],
            "n_verified": len(data.get("verified", [])),
            "n_dropped": len(data.get("dropped", []))}


def n_render(state: GraphState) -> dict:
    """Stage 5 — T.render: always a table, with a status per institution,
    written to rezultate_graph/ (never clobbering teatre.py's rezultate/)."""
    t0 = time.monotonic()
    T.render(T.parse_period(state["period"]))
    return {"timings": [("render", time.monotonic() - t0)]}


# ─────────────────────────────────────────────────────────── graph
def build_graph(checkpointer=None):
    g = StateGraph(GraphState)

    for name, fn in [("preflight", n_preflight), ("abort", n_abort),
                     ("plan", n_plan), ("fetch", n_fetch),
                     ("diagnose", n_diagnose), ("extract_one", n_extract_one),
                     ("verify", n_verify), ("render", n_render)]:
        g.add_node(name, fn)

    g.add_edge(START, "preflight")
    g.add_conditional_edges("preflight", r_after_preflight, ["abort", "plan"])
    g.add_edge("abort", END)

    g.add_edge("plan", "fetch")
    g.add_edge("fetch", "diagnose")

    # Map -> (implicit barrier) -> reduce.
    g.add_conditional_edges("diagnose", fan_out_extract, ["extract_one", "verify"])
    g.add_edge("extract_one", "verify")

    g.add_edge("verify", "render")
    g.add_edge("render", END)

    # A checkpointer persists state after EVERY node. Re-invoking with the same
    # thread_id resumes from the last completed node instead of re-fetching and
    # re-paying for extraction. SqliteSaver (teatre_graph_checkpoints.db) makes
    # --resume survive a process restart; InMemorySaver is the fallback.
    return g.compile(checkpointer=checkpointer)


def _run(graph, args: argparse.Namespace, model: str) -> int:
    t0 = time.monotonic()
    final = graph.invoke(
        {"period": str(T.parse_period(args.period)), "backend": args.backend,
         "model": model, "concurrency": args.concurrency,
         "results": [], "timings": []},
        {"configurable": {"thread_id": args.thread}, "recursion_limit": 200})

    total = time.monotonic() - t0
    print(f"\n{'=' * 64}\n  GRAPH RUN — {T.fmt_dur(total)}\n{'=' * 64}")
    results = final.get("results", [])
    n_rows = sum(len(r.get("rows", [])) for r in results)
    n_det = sum(1 for r in results if r.get("method"))
    n_model = sum(1 for r in results
                  if r.get("status") in ("OK", "EXTRACT_FAILED") and not r.get("method"))
    print(f"  extractions {len(results)} ({n_det} deterministic, {n_model} model) · "
          f"raw rows {n_rows} · verified {final.get('n_verified', '?')} · "
          f"dropped {final.get('n_dropped', '?')}")
    for name, sec in final.get("timings", []):
        if not name.startswith("extract:"):
            print(f"    {name:<10} {T.fmt_dur(sec):>9}")
    print(f"\n  outputs -> {T.SNAPDIR}/  and  {T.OUTDIR}/")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description="teatre.py pipeline as a LangGraph graph "
                    "(outputs isolated in snapshots_graph/ + rezultate_graph/)")
    ap.add_argument("--period", default="current-month")
    ap.add_argument("--backend", default="none",
                    choices=["none", "ollama", "cerebras", "mimo"])
    ap.add_argument("--model", default="")
    ap.add_argument("--concurrency", type=int, default=4,
                    help="parallel curl workers in harvest (default 4)")
    ap.add_argument("--extract-concurrency", type=int, default=2,
                    help="semaphore cap on parallel model calls (default 2 — "
                         "API tiers limit tokens/minute, not requests)")
    ap.add_argument("--thread", default="teatru-graph",
                    help="checkpoint thread id (same id + --resume = continue)")
    ap.add_argument("--resume", action="store_true",
                    help="resume a previously interrupted run (same --thread)")
    ap.add_argument("--draw", action="store_true", help="print the graph and exit")
    args = ap.parse_args()

    global EXTRACT_SEM
    EXTRACT_SEM = threading.Semaphore(max(1, args.extract_concurrency))

    model = args.model or {"ollama": "gemma3:27b", "cerebras": "gpt-oss-120b",
                           "mimo": "xiaomi/mimo-v2.5"}.get(args.backend, "")

    if args.draw:
        graph = build_graph(InMemorySaver())
        print(graph.get_graph().draw_mermaid())
        return 0

    if SqliteSaver is not None:
        # Real persistence: --resume reuses the stored thread. Without --resume
        # we mint a unique thread id so a finished run is never accidentally
        # "resumed" from its terminal checkpoint.
        thread = args.thread if args.resume else f"{args.thread}-{int(time.time())}"
        with SqliteSaver.from_conn_string(str(CHECKPOINT_DB)) as saver:
            graph = build_graph(saver)
            args.thread = thread
            return _run(graph, args, model)
    graph = build_graph(InMemorySaver())
    print("  (langgraph-checkpoint-sqlite missing — using in-memory checkpoints; "
          "--resume only works within one process)")
    return _run(graph, args, model)


if __name__ == "__main__":
    raise SystemExit(main())
