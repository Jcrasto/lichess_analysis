"""Background runner for generating deterministic game reviews."""
import asyncio
import logging
import threading
from collections import deque
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("review_runner")


class ReviewRunner:
    """Singleton that generates game reviews in a background thread.
    Mirrors EvalRunner: buffers log lines and fans them to SSE subscribers.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._counter_lock = threading.Lock()
        self.is_running: bool = False
        self._stop_requested: bool = False
        self.username: Optional[str] = None
        self.processed: int = 0
        self.failed: int = 0
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
        with self._counter_lock:
            return {
                "running": self.is_running,
                "username": self.username,
                "processed": self.processed,
                "failed": self.failed,
                "total_queued": self.total_queued,
                "started_at": self.started_at.isoformat() if self.started_at else None,
                "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            }

    def start(self, username: str, filters: dict = None, force: bool = False) -> bool:
        """Start review generation in background thread. Returns False if already running."""
        with self._lock:
            if self.is_running:
                return False
            self.is_running = True
            self._stop_requested = False
            self.username = username
            self.processed = 0
            self.failed = 0
            self.total_queued = 0
            self.started_at = datetime.now(timezone.utc)
            self.finished_at = None

        t = threading.Thread(
            target=self._run,
            args=(username, filters or {}, force),
            daemon=True,
        )
        t.start()
        return True

    def stop(self):
        self._stop_requested = True
        self._emit("Stop requested — finishing current game…")

    # ── internal ──────────────────────────────────────────────────────────────

    def _run(self, username: str, filters: dict, force: bool = False):
        try:
            from storage import get_unreviewed_games, query_evals_for_game, upsert_review
            from review_generator import generate_review

            games = get_unreviewed_games(username, **filters, force=force)
            if not games:
                self._emit("No games need reviews — all evaluated games are already reviewed.")
                return

            with self._counter_lock:
                self.total_queued = len(games)
            self._emit(f"Found {len(games)} games to review — starting…")

            for idx, game in enumerate(games, start=1):
                if self._stop_requested:
                    self._emit(f"Stopped — {self.processed} reviewed, {self.failed} failed.")
                    return

                game_id = game["game_id"]
                date_str = str(game.get("date", "?"))
                self._emit(f"[{idx}/{len(games)}] → {game_id}  ({date_str})")

                try:
                    evals = query_evals_for_game(username, game_id)
                    review = generate_review(game, evals, username)
                    review["game_id"] = game_id
                    review["date"] = game.get("date")
                    review["year"] = game.get("year")
                    review["month"] = game.get("month")
                    review["reviewed_at"] = datetime.now(timezone.utc).isoformat()
                    upsert_review(username, review)

                    with self._counter_lock:
                        self.processed += 1
                    self._emit(
                        f"[{idx}/{len(games)}] ✓ {game_id}  "
                        f"{review['blunder_count']}B {review['mistake_count']}M "
                        f"{review['inaccuracy_count']}I"
                    )
                except Exception as e:
                    with self._counter_lock:
                        self.failed += 1
                    self._emit(f"[{idx}/{len(games)}] ✗ {game_id}  {e}")
                    logger.exception("Review generation failed for %s", game_id)

            self._emit(f"Complete — {self.processed} reviewed, {self.failed} failed.")
        except Exception as e:
            self._emit(f"ERROR: {e}")
            logger.exception("Review run failed")
        finally:
            with self._lock:
                self.is_running = False
                self.finished_at = datetime.now(timezone.utc)


# module-level singleton
runner = ReviewRunner()
