#!/usr/bin/env python3
"""
teatre_viz_server.py — local observability broker for a live teatre_graph run.

Serves teatre_graph_viz_v2.html (the live visualizer) and brokbps a real run:

    GET  /            -> the v2 visualizer page
    GET  /api/state?since=SEQ  -> JSON snapshot of events/log after SEQ
    POST /api/run     {period, backend?, model?, resume?, thread?} -> spawn
    POST /api/abort             -> terminate the current run

The server spawns ``python3 -u teatre_viz_driver.py`` and reads its stdout.
Lines prefixed ``VIS\\t`` are machine lifecycle events (see the driver); every
other line is the run's own human log. The browser polls /api/state and animates
nodes/edges from the real event sequence.

Stdlib only: http.server + threading + subprocess. No framework, no install.

Usage:
    python3 teatre_viz_server.py [port]     # default 8123
    open http://127.0.0.1:8123/
"""

from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import sys
import threading
import time
import urllib.parse

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = pathlib.Path(__file__).resolve().parent
VIS_HTML = HERE / "teatre_graph_viz_v2.html"
GRAPH_RESULTS = HERE / "rezultate_graph"

CT_HTML = "text/html; charset=utf-8"
CT_JSON = "application/json; charset=utf-8"


def events_for(period: str) -> dict:
    """Return the verified event rows for a month from rezultate_graph/.

    Missing/empty output -> exists=False so the UI can say nothing was
    rendered for that period yet. Only the kept (verified) rows come back;
    the dropped rows are counted, not shipped, to keep the payload lean.
    """
    path = GRAPH_RESULTS / f"{period}.verified.json"
    if not path.exists():
        return {"period": period, "exists": False, "verified": [], "dropped": 0}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"period": period, "exists": False, "verified": [], "dropped": 0}
    return {"period": period, "exists": True,
            "verified": data.get("verified", []),
            "dropped": len(data.get("dropped", []))}


def project_python() -> str:
    """The venv python that has langgraph installed; fall back to this interpreter."""
    for cand in (HERE / ".venv" / "bin" / "python",
                 HERE / ".venv" / "bin" / "python3"):
        if cand.exists():
            return str(cand)
    return sys.executable


class Run:
    """One live graph run: the child process plus everything it has emitted."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.proc: subprocess.Popen | None = None
        self.seq = 0
        self.events: list[dict] = []             # VIS lifecycle events, in order
        self.lines: list[tuple[int, str]] = []  # (seq, human log line)
        self.meta: dict = {}
        self.venues: list[dict] = []
        self.exit_code: int | None = None
        self.stderr = ""
        self.started = 0.0

    def snapshot(self, since: int = 0) -> dict:
        with self.lock:
            return {
                "running": self.proc is not None and self.proc.poll() is None,
                "seq": self.seq,
                "events": [e for e in self.events if e["seq"] > since],
                "log": [t for s, t in self.lines if s > since],
                "meta": self.meta,
                "venues": self.venues,
                "exit_code": self.exit_code,
                "stderr": self.stderr[-1000:],
                "elapsed": round(time.monotonic() - self.started, 1) if self.started else 0,
            }

    def start(self, argv: list[str]) -> str | None:
        """Launch the driver subprocess. Returns an error string or None."""
        with self.lock:
            if self.proc is not None and self.proc.poll() is None:
                return "a run is already in progress"
            self.__init__()
            self.proc = subprocess.Popen(
                [project_python(), "-u", str(HERE / "teatre_viz_driver.py"), *argv],
                cwd=str(HERE), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, bufsize=1)
            self.started = time.monotonic()
            threading.Thread(target=self._pump, args=(self.proc,), daemon=True).start()
            return None

    def _pump(self, proc: subprocess.Popen) -> None:
        assert proc.stdout is not None
        for raw in proc.stdout:
            if raw.startswith("VIS\t"):
                try:
                    self._add(json.loads(raw[4:]))
                except json.JSONDecodeError:
                    self._log(raw)
            else:
                self._log(raw)
        # EOF on stdout -> the child has flushed and is finishing; wait() for
        # the definitive exit code (poll() can race and return None here).
        code = proc.wait()
        err = ""
        if proc.stderr:
            try:
                err = proc.stderr.read()
            except OSError:
                err = ""
        with self.lock:
            self.exit_code = code
            self.stderr = err
            self.seq += 1
            self.events.append({"t": "end", "seq": self.seq,
                                "exit_code": code, "stderr": err[-1000:]})

    def _add(self, ev: dict) -> None:
        with self.lock:
            self.seq += 1
            ev = dict(ev)
            ev["seq"] = self.seq
            self.events.append(ev)
            self._route(ev)

    def _log(self, raw: str) -> None:
        text = raw.rstrip("\n")
        if not text:
            return
        with self.lock:
            self.seq += 1
            self.lines.append((self.seq, text))

    def _route(self, ev: dict) -> None:
        t = ev.get("t")
        if t == "meta":
            self.meta = ev
        elif t == "venues":
            self.venues = ev.get("venues", [])

    def abort(self) -> None:
        with self.lock:
            if self.proc is not None and self.proc.poll() is None:
                self.proc.terminate()

    def alive(self) -> bool:
        with self.lock:
            return self.proc is not None and self.proc.poll() is None


RUN = Run()


def run_argv(body: dict) -> list[str]:
    """Build teatre_viz_driver argv from a POST body."""
    period = body.get("period") or body.get("label") or "current-month"
    argv = ["--period", str(period)]
    backend = str(body.get("backend") or "none")
    if backend in ("none", "ollama", "cerebras", "mimo"):
        argv += ["--backend", backend]
    if body.get("model"):
        argv += ["--model", str(body["model"])]
    if body.get("concurrency"):
        argv += ["--concurrency", str(body["concurrency"])]
    if body.get("extract_concurrency"):
        argv += ["--extract-concurrency", str(body["extract_concurrency"])]
    if body.get("thread"):
        argv += ["--thread", str(body["thread"])]
    if body.get("resume"):
        argv += ["--resume"]
    return argv


class Handler(BaseHTTPRequestHandler):
    def _text(self, body: bytes, ctype: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        # The page may be opened directly from disk (file://): browsers treat
        # that origin as `null`, so allow cross-origin reads of /api/state.
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, status: int = 200) -> None:
        self._text(json.dumps(obj).encode("utf-8"), CT_JSON, status)

    def _html(self) -> None:
        if not VIS_HTML.exists():
            self._text(b"teatre_graph_viz_v2.html missing next to server", CT_HTML, 404)
            return
        self._text(VIS_HTML.read_text(encoding="utf-8").encode("utf-8"), CT_HTML)

    def do_OPTIONS(self) -> None:
        # Preflight for POST from a file:// (null origin) page.
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0]
        if path in ("/", "/index.html", "/teatre_graph_viz_v2.html"):
            return self._html()
        if path == "/api/state":
            since = 0
            q = self.path.partition("?")[2]
            if q.startswith("since="):
                try:
                    since = int(q[len("since="):])
                except ValueError:
                    since = 0
            return self._json(RUN.snapshot(since))
        if path == "/api/events":
            params = urllib.parse.parse_qs(self.path.partition("?")[2])
            period = (params.get("period") or [""])[0]
            return self._json(events_for(period))
        self._json({"error": "not found"}, 404)

    def do_POST(self) -> None:
        path = self.path.split("?", 1)[0]
        try:
            n = int(self.headers.get("Content-Length", "0") or 0)
        except ValueError:
            n = 0
        raw = self.rfile.read(n) if n else b""
        if path == "/api/run":
            try:
                body = json.loads(raw or b"{}")
            except json.JSONDecodeError:
                return self._json({"error": "bad json"}, 400)
            err = RUN.start(run_argv(body))
            return self._json({"error": err} if err else {"ok": True, "seq": RUN.seq},
                              status=409 if err else 200)
        if path == "/api/abort":
            RUN.abort()
            return self._json({"ok": True})
        self._json({"error": "not found"}, 404)

    def log_message(self, *a):
        pass


def main() -> None:
    ap = argparse.ArgumentParser(description="Live observability broker for teatre_graph")
    ap.add_argument("port", nargs="?", type=int, default=8123)
    args = ap.parse_args()
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"teatre viz server → http://127.0.0.1:{args.port}  (serve {VIS_HTML.name})")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        RUN.abort()
        raise SystemExit(0)


if __name__ == "__main__":
    main()