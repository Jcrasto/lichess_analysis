import asyncio
import logging
import threading
from collections import deque
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("eval_runner")


class EvalRunner:
    """
    Singleton that runs Stockfish evaluation in a background thread.
    Buffers log lines and fans them out to SSE subscribers so clients
    can connect, disconnect, and reconnect freely.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self.is_running: bool = False
        self._stop_requested: bool = False
        self.username: Optional[str] = None
        self.depth: int = 15
        self.evaluated: int = 0
        self.failed: int = 0
        self.remaining: int = 0
        self.total_queued: int = 0
        self.started_at: Optional[datetime] = None
        self.finished_at: Optional[datetime] = None
        self.log_buffer: deque = deque(maxlen=2000)
        self._subscribers: set = set()
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def set_loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop

    # ── logging ───────────────────────────────────────────────────────────────

    def _emit(self, msg: str):
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        line = f"{ts}  {msg}"
        self.log_buffer.append(line)
        logger.info(msg)
        if self._loop and not self._loop.is_closed():
            for q in list(self._subscribers):
                self._loop.call_soon_threadsafe(q.put_nowait, line)

    # ── SSE subscriptions ─────────────────────────────────────────────────────

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        self._subscribers.discard(q)

    # ── public API ────────────────────────────────────────────────────────────

    def get_status(self) -> dict:
        return {
            "running": self.is_running,
            "username": self.username,
            "evaluated": self.evaluated,
            "failed": self.failed,
            "remaining": self.remaining,
            "total_queued": self.total_queued,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
        }

    def start(self, username: str, depth: int = 15) -> bool:
        """Start evaluation in background thread. Returns False if already running."""
        with self._lock:
            if self.is_running:
                return False
            self.is_running = True
            self._stop_requested = False
            self.username = username
            self.depth = depth
            self.evaluated = 0
            self.failed = 0
            self.remaining = 0
            self.total_queued = 0
            self.started_at = datetime.now(timezone.utc)
            self.finished_at = None

        t = threading.Thread(target=self._run, args=(username, depth), daemon=True)
        t.start()
        return True

    def stop(self):
        """Signal the running evaluation to stop after the current game."""
        self._stop_requested = True
        self._emit("Stop requested — will finish after current game…")

    # ── internal ──────────────────────────────────────────────────────────────

    def _on_progress(self, event: dict):
        etype = event["type"]
        if etype == "queued":
            self.total_queued = event["total"]
            self.remaining = event["total"]
            self._emit(f"Found {event['total']} unevaluated games — starting (depth={self.depth})")
        elif etype == "start":
            self._emit(
                f"[{event['index']}/{event['total']}] → {event['game_id']}  ({event['date']})"
            )
        elif etype == "done":
            self.evaluated += 1
            self.remaining = max(0, event["total"] - event["index"])
            self._emit(
                f"[{event['index']}/{event['total']}] ✓ {event['game_id']}  {event['plies']} plies"
            )
        elif etype == "failed":
            self.failed += 1
            self.remaining = max(0, event["total"] - event["index"])
            self._emit(
                f"[{event['index']}/{event['total']}] ✗ {event['game_id']}  {event.get('error', '')}"
            )

    def _run(self, username: str, depth: int):
        try:
            from evaluator import run_evaluation_all
            result = run_evaluation_all(
                username,
                depth=depth,
                progress_callback=self._on_progress,
                stop_check=lambda: self._stop_requested,
            )
            if self._stop_requested:
                self._emit(
                    f"Stopped — {result['evaluated']} evaluated, "
                    f"{result['failed']} failed, {result['remaining']} remaining"
                )
            else:
                self._emit(
                    f"Complete — {result['evaluated']} evaluated, "
                    f"{result['failed']} failed, {result['remaining']} remaining"
                )
        except Exception as e:
            self._emit(f"ERROR: {e}")
            logger.exception("Evaluation run failed")
        finally:
            with self._lock:
                self.is_running = False
                self.finished_at = datetime.now(timezone.utc)


# module-level singleton
runner = EvalRunner()
