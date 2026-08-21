#!/usr/bin/env python3
"""
teatre_viz_driver.py — run teatre_graph.py under a thin observability harness.

Spawns the SAME LangGraph pipeline (imported, not forked) but wraps every node
and conditional edge in a tiny envelope that emits one machine-readable JSON
event per lifecycle transition on stdout, one JSON object per line, each
prefixed with the sentinel ``VIS\\t`` so a supervisor server can separate the
live graph trace from the process's ordinary human-facing log output.

The wrappers call the real node functions and return their real values; nothing
about the graph's behaviour changes. This is observability, not orchestration.

Event vocabulary (the value of "t"):

    meta        {period, backend, model, thread, started_iso}
    venues      {venues: [{slug, name, strategy, part}]}   worker roster
    node_start  {node, slug?}                              a node is entering
    node_done   {node, slug?, partial}                     node returned its update
    node_error  {node, slug?, error}                       node raised
    edge        {edge: "preflight_plan"|"preflight_abort"} conditional edge fired
    fanout      {n}                                        diagnose returned list[Send]

The driver reimplements teatre_graph.main()'s tail (thread naming, SqliteSaver
when available, model default) so a reserved run is checkpoint-resumable and
writes to the same isolated snapshots_graph/ + rezultate_graph/ the user's own
`teatre_graph.py` uses.

Run directly (for the server):
    python3 teatre_viz_driver.py --period 2026-09 --backend none
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time

import teatre as T
import teatre_graph as tg


# ---------------------------------------------------------------- emit
def emit(payload: dict) -> None:
    """One JSON lifecycle event, VIS-prefixed, flushed immediately."""
    print("VIS\t" + json.dumps(payload, ensure_ascii=False), flush=True)


class LineLockedOut:
    """Thread-safe stdout replacement: one line = one atomic write.

    Send() workers print from many threads while the wrappers emit() VIS
    events; print() writes text and newline as separate calls, so without
    serialization one thread's line could land mid-way through another's and
    corrupt the VIS framing (the server splits the stream on newlines).

    Each thread keeps its OWN partial-line buffer; a newline only ever flushes
    that thread's own text, so interleaved print() calls can never merge two
    threads' partial text into one line. Lines are emitted under one lock and
    flushed to the pipe immediately (the server depends on near-real-time
    delivery for a live view).
    """

    def __init__(self, real):
        self._real = real
        self._lock = threading.Lock()
        self._bufs: dict[int, str] = {}

    def write(self, s: str) -> None:
        with self._lock:
            tid = threading.get_ident()
            buf = self._bufs.get(tid, "") + s
            while "\n" in buf:
                line, buf = buf.split("\n", 1)
                self._real.write(line + "\n")
                self._real.flush()
            if buf:
                self._bufs[tid] = buf
            else:
                self._bufs.pop(tid, None)

    def flush(self) -> None:
        with self._lock:
            for tid, buf in list(self._bufs.items()):
                if buf:
                    self._real.write(buf + "\n")
                    self._bufs[tid] = ""
            self._real.flush()

    def isatty(self) -> bool:
        return False


def _slug_of(args) -> str | None:
    """extract_one receives an ExtractTask dict as its only positional arg."""
    try:
        first = args[0]
        return first.get("slug") if isinstance(first, dict) else None
    except (IndexError, AttributeError):
        return None


def _wrap(name: str, fn):
    """Return a wrapper that reports start/done/error around the real node."""
    def inner(*args, **kwargs):
        slug = _slug_of(args) if name == "extract_one" else None
        emit({"t": "node_start", "node": name, "slug": slug})
        t0 = time.monotonic()
        try:
            res = fn(*args, **kwargs)
        except Exception as exc:
            emit({"t": "node_error", "node": name, "slug": slug,
                  "error": str(exc)[:400]})
            raise
        partial = res if isinstance(res, dict) else {}
        emit({"t": "node_done", "node": name, "slug": slug, "partial": partial})
        return res
    return inner


def _wrap_edge(name: str, f):
    """Edge wrappers report which branch a conditional actually fired."""
    def inner(state):
        res = f(state)
        if name == "r_after_preflight":
            emit({"t": "edge", "edge": "preflight_abort" if res == "abort"
                  else "preflight_plan"})
        elif name == "fan_out_extract" and isinstance(res, list):
            emit({"t": "fanout", "n": len(res)})
        return res
    return inner


def _install_wrappers() -> None:
    """Reassign module attrs BEFORE build_graph() reads them.

    build_graph() does ``for name, fn in [("preflight", n_preflight), ...]``
    and ``add_conditional_edges("preflight", r_after_preflight, ...)`` at call
    time, so reassigning the module attribute before invoking build_graph()
    is enough — Send("extract_one", ...) resolves the node by registered name,
    which is already the wrapper.
    """
    import teatre_graph as tg
    for name in ("preflight", "abort", "plan", "fetch", "diagnose",
                 "extract_one", "verify", "render"):
        setattr(tg, "n_" + name, _wrap(name, getattr(tg, "n_" + name)))
    setattr(tg, "r_after_preflight", _wrap_edge("r_after_preflight",
                                                tg.r_after_preflight))
    setattr(tg, "fan_out_extract", _wrap_edge("fan_out_extract",
                                              tg.fan_out_extract))
    # Seed the roster once here so the UI can render workers even before the
    # first fanout event arrives.
    emit({"t": "venues", "venues": [
        {"slug": i["slug"], "name": i.get("name", i["slug"]),
         "strategy": i.get("strategy", "html"),
         "part": "all" if not T.is_month_parameterised(i.get("calendar_url", ""))
                 else "month"}
        for i in T.load_sources(include_disabled=False)]})


# ---------------------------------------------------------------- driving
def _build_run(args: argparse.Namespace, model: str):
    """Mirror teatre_graph.main()'s checkpoint wiring, but with wrappers live."""
    import teatre_graph as tg
    if tg.SqliteSaver is not None:
        thread = args.thread if args.resume else f"{args.thread}-{int(time.time())}"
        with tg.SqliteSaver.from_conn_string(str(tg.CHECKPOINT_DB)) as saver:
            graph = tg.build_graph(saver)
            args.thread = thread
            return tg._run(graph, args, model)
    graph = tg.build_graph(tg.InMemorySaver())
    return tg._run(graph, args, model)


def main() -> int:
    # One line = one pipe write; per-thread buffers keep worker prints from
    # merging and from corrupting the VIS framing.
    sys.stdout = LineLockedOut(sys.stdout)
    ap = argparse.ArgumentParser(description="Live-observable teatre_graph run")
    ap.add_argument("--period", default="current-month")
    ap.add_argument("--backend", default="none",
                    choices=["none", "ollama", "cerebras", "mimo"])
    ap.add_argument("--model", default="")
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--extract-concurrency", type=int, default=2)
    ap.add_argument("--thread", default="teatru-graph")
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    tg = __import__("teatre_graph")
    tg.EXTRACT_SEM = threading.Semaphore(max(1, args.extract_concurrency))
    model = args.model or {"ollama": "gemma3:27b", "cerebras": "gpt-oss-120b",
                           "mimo": "xiaomi/mimo-v2.5"}.get(args.backend, "")

    emit({"t": "meta", "period": str(T.parse_period(args.period)),
          "backend": args.backend, "model": model, "thread": args.thread,
          "started_iso": time.strftime("%Y-%m-%dT%H:%M:%S")})

    _install_wrappers()
    return _build_run(args, model)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)