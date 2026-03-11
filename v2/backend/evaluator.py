import io
import logging
import os
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import chess
import chess.engine
import chess.pgn
import pandas as pd

logger = logging.getLogger("evaluator")

_thread_local = threading.local()


def _normalize_evals_df(df: pd.DataFrame) -> pd.DataFrame:
    """Enforce consistent column dtypes to avoid FutureWarning on concat."""
    if df.empty:
        return df
    if "move_number" in df.columns:
        df = df.copy()
        df["move_number"] = df["move_number"].astype("int32")
    if "cp_score" in df.columns:
        df["cp_score"] = df["cp_score"].astype("float64")
    if "mate_in" in df.columns:
        df["mate_in"] = pd.to_numeric(df["mate_in"], errors="coerce").astype("Int32")
    return df


from storage import (
    games_parquet_exists,
    get_games_dir,
    load_eval_month_parquet,
    write_eval_month_parquet,
    load_month_parquet,
    write_month_parquet,
    query_unevaluated_count,
    query_unevaluated_games,
)

STOCKFISH_CANDIDATES = [
    "/opt/homebrew/bin/stockfish",
    "/usr/local/bin/stockfish",
    "/usr/bin/stockfish",
]


def get_stockfish_path() -> Optional[str]:
    found = shutil.which("stockfish")
    if found:
        return found
    for p in STOCKFISH_CANDIDATES:
        if Path(p).exists():
            return p
    return None


# ── Thread-local engine management ────────────────────────────────────────────

def _get_thread_engine(sf_path: str) -> chess.engine.SimpleEngine:
    """Return the thread-local Stockfish engine, creating it if needed."""
    if getattr(_thread_local, "engine", None) is None:
        _thread_local.engine = chess.engine.SimpleEngine.popen_uci(sf_path)
    return _thread_local.engine


def _reset_thread_engine(sf_path: str) -> chess.engine.SimpleEngine:
    """Kill the thread-local engine and start a fresh one."""
    engine = getattr(_thread_local, "engine", None)
    if engine is not None:
        try:
            engine.quit()
        except Exception:
            pass
    _thread_local.engine = chess.engine.SimpleEngine.popen_uci(sf_path)
    return _thread_local.engine


def _close_thread_engine():
    """Quit and clear the thread-local engine (called on worker shutdown)."""
    engine = getattr(_thread_local, "engine", None)
    if engine is not None:
        try:
            engine.quit()
        except Exception:
            pass
        _thread_local.engine = None


# ── Core evaluation helpers ────────────────────────────────────────────────────

def _eval_pgn_with_engine(engine: chess.engine.SimpleEngine, pgn: str, game_id: str, depth: int) -> list:
    """Evaluate a PGN using a provided engine instance. Does not close the engine."""
    pgn_io = io.StringIO(pgn)
    chess_game = chess.pgn.read_game(pgn_io)
    if chess_game is None:
        return []

    date_obj = None
    date_str = chess_game.headers.get("Date", "")
    try:
        date_obj = datetime.strptime(
            date_str.replace(".??", "").replace("??", "01"), "%Y.%m.%d"
        ).date()
    except Exception:
        pass

    year = date_obj.year if date_obj else None
    month = date_obj.month if date_obj else None

    board = chess_game.board()
    evals = []
    now = datetime.now(timezone.utc)

    for move_number, move in enumerate(chess_game.mainline_moves(), start=1):
        board.push(move)
        info = engine.analyse(board, chess.engine.Limit(depth=depth))
        score = info["score"].white()

        cp_score = None
        mate_in = None
        if score.is_mate():
            mate_in = score.mate()
        else:
            cp = score.score()
            if cp is not None:
                cp_score = cp / 100.0

        best_move_obj = info.get("pv", [None])[0]
        best_move = best_move_obj.uci() if best_move_obj else None

        evals.append(
            {
                "game_id": game_id,
                "date": date_obj,
                "year": year,
                "month": month,
                "move_number": move_number,
                "fen": board.fen(),
                "cp_score": cp_score,
                "mate_in": mate_in,
                "best_move": best_move,
                "evaluated_at": now,
            }
        )

    return evals


def evaluate_game(pgn: str, game_id: str, depth: int = 15) -> list:
    """Run Stockfish on a game PGN. Spawns and quits its own engine.
    Kept for single-game / external callers. Use _evaluate_reuse in batch loops."""
    sf_path = get_stockfish_path()
    if not sf_path:
        raise RuntimeError("Stockfish not found. Install via: brew install stockfish")
    engine = chess.engine.SimpleEngine.popen_uci(sf_path)
    try:
        return _eval_pgn_with_engine(engine, pgn, game_id, depth)
    finally:
        engine.quit()


def _evaluate_reuse(sf_path: str, pgn: str, game_id: str, depth: int) -> list:
    """Evaluate using the thread-local engine. Resets the engine if it crashes."""
    try:
        engine = _get_thread_engine(sf_path)
        return _eval_pgn_with_engine(engine, pgn, game_id, depth)
    except chess.engine.EngineError:
        logger.warning("Engine crashed for %s — restarting thread engine", game_id)
        engine = _reset_thread_engine(sf_path)
        return _eval_pgn_with_engine(engine, pgn, game_id, depth)


def _evaluate_with_retry(pgn: str, game_id: str, depth: int, max_retries: int = 3) -> list:
    """Call evaluate_game (own engine) with retries. Used only in single-game paths."""
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            rows = evaluate_game(pgn, game_id, depth=depth)
            if rows:
                return rows
            logger.warning("Attempt %d/%d: no rows for %s", attempt, max_retries, game_id)
        except Exception as e:
            last_exc = e
            logger.warning("Attempt %d/%d failed for %s: %s", attempt, max_retries, game_id, e)
        if attempt < max_retries:
            time.sleep(2 ** (attempt - 1))
    if last_exc:
        raise last_exc
    return []


def _evaluate_reuse_with_retry(sf_path: str, pgn: str, game_id: str, depth: int, max_retries: int = 3) -> list:
    """Evaluate using thread-local engine with retries."""
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            rows = _evaluate_reuse(sf_path, pgn, game_id, depth)
            if rows:
                return rows
            logger.warning("Attempt %d/%d: no rows for %s", attempt, max_retries, game_id)
        except Exception as e:
            last_exc = e
            logger.warning("Attempt %d/%d failed for %s: %s", attempt, max_retries, game_id, e)
        if attempt < max_retries:
            time.sleep(2 ** (attempt - 1))
    if last_exc:
        raise last_exc
    return []


# ── Parallel evaluation loop ───────────────────────────────────────────────────

def _run_eval_loop(
    username: str,
    rows: list,
    depth: int,
    progress_callback=None,
    stop_check=None,
    workers: int = 4,
) -> dict:
    """Evaluate games in parallel using a thread pool.

    rows = [(game_id, pgn, year, month, date), ...]
    Each worker thread maintains a persistent Stockfish engine.
    Parquet writes are serialized via a lock.
    """
    sf_path = get_stockfish_path()
    if not sf_path:
        raise RuntimeError("Stockfish not found")

    total = len(rows)
    write_lock = threading.Lock()
    # Thread-safe counters
    counter_lock = threading.Lock()
    evaluated = [0]
    failed = [0]

    if progress_callback:
        progress_callback({"type": "queued", "total": total})

    def process_one(idx: int, row: tuple):
        """Run in a worker thread. Returns a result dict."""
        game_id, pgn, year, month, game_date = row
        date_str = str(game_date) if game_date else "unknown"

        if stop_check and stop_check():
            return {"status": "stopped", "idx": idx, "game_id": game_id,
                    "date_str": date_str, "year": year, "month": month}

        if progress_callback:
            progress_callback({"type": "start", "index": idx, "total": total,
                               "game_id": game_id, "date": date_str})
        try:
            eval_rows = _evaluate_reuse_with_retry(sf_path, pgn, game_id, depth=depth)
            return {"status": "ok", "idx": idx, "game_id": game_id,
                    "date_str": date_str, "year": year, "month": month,
                    "eval_rows": eval_rows}
        except Exception as e:
            return {"status": "error", "idx": idx, "game_id": game_id,
                    "date_str": date_str, "year": year, "month": month,
                    "error": str(e)}

    actual_workers = min(workers, total, os.cpu_count() or 1)
    logger.info("Starting parallel evaluation: %d games, %d workers, depth=%d", total, actual_workers, depth)

    with ThreadPoolExecutor(max_workers=actual_workers) as pool:
        futures = {
            pool.submit(process_one, i, row): i
            for i, row in enumerate(rows, start=1)
        }

        for future in as_completed(futures):
            try:
                res = future.result()
            except Exception as e:
                # Unexpected — log and move on
                logger.error("Unexpected future error: %s", e, exc_info=True)
                with counter_lock:
                    failed[0] += 1
                continue

            idx = res["idx"]
            game_id = res["game_id"]
            date_str = res["date_str"]
            year = res["year"]
            month = res["month"]

            if res["status"] == "stopped":
                continue

            if res["status"] == "error" or not res.get("eval_rows"):
                with counter_lock:
                    failed[0] += 1
                error_msg = res.get("error", "no plies returned")
                logger.error("[%d/%d] Permanently failed %s: %s", idx, total, game_id, error_msg)
                if progress_callback:
                    progress_callback({"type": "failed", "index": idx, "total": total,
                                       "game_id": game_id, "error": error_msg})
                continue

            eval_rows = res["eval_rows"]
            try:
                with write_lock:
                    df_evals = _normalize_evals_df(pd.DataFrame(eval_rows))
                    existing_evals = load_eval_month_parquet(username, int(year), int(month))
                    if existing_evals.empty:
                        combined_evals = df_evals
                    else:
                        combined_evals = pd.concat(
                            [_normalize_evals_df(existing_evals), df_evals], ignore_index=True
                        ).drop_duplicates(subset=["game_id", "move_number"], keep="last")
                    write_eval_month_parquet(username, int(year), int(month), combined_evals.reset_index(drop=True))

                    df_games = load_month_parquet(username, int(year), int(month))
                    if not df_games.empty:
                        df_games.loc[df_games["game_id"] == game_id, "has_eval"] = True
                        write_month_parquet(username, int(year), int(month), df_games)

                with counter_lock:
                    evaluated[0] += 1

                if progress_callback:
                    progress_callback({"type": "done", "index": idx, "total": total,
                                       "game_id": game_id, "date": date_str,
                                       "plies": len(eval_rows)})
            except Exception as e:
                with counter_lock:
                    failed[0] += 1
                logger.error("[%d/%d] Write failed for %s: %s", idx, total, game_id, e, exc_info=True)
                if progress_callback:
                    progress_callback({"type": "failed", "index": idx, "total": total,
                                       "game_id": game_id, "error": f"write error: {e}"})

    # Shut down all thread-local engines
    pool.shutdown(wait=False)

    remaining = query_unevaluated_count(username)
    return {"evaluated": evaluated[0], "failed": failed[0], "remaining": remaining}


def _query_unevaluated(username: str, limit: Optional[int] = None, **filters) -> list:
    """Query unevaluated games, optionally with game filters."""
    rows = query_unevaluated_games(username, **filters)
    if limit:
        rows = rows[:limit]
    return rows


def _query_unevaluated_legacy(username: str, limit: Optional[int] = None) -> list:
    import duckdb
    glob_path = str(get_games_dir(username) / "*.parquet")
    limit_clause = f"LIMIT {limit}" if limit else ""
    con = duckdb.connect()
    rows = con.execute(
        f"""SELECT game_id, pgn, year, month, date FROM read_parquet('{glob_path}')
            WHERE has_eval = false
            ORDER BY date DESC
            {limit_clause}"""
    ).fetchall()
    con.close()
    return rows


def _default_workers() -> int:
    """Physical core count — optimal for CPU-bound Stockfish processes."""
    return max(1, (os.cpu_count() or 2) // 2)


def run_evaluation_all(
    username: str,
    depth: int = 15,
    progress_callback=None,
    stop_check=None,
    workers: Optional[int] = None,
    **filters,
) -> dict:
    """Evaluate unevaluated games matching filters (or all if no filters), newest first."""
    if workers is None:
        workers = _default_workers()
    if not games_parquet_exists(username):
        return {"evaluated": 0, "failed": 0, "remaining": 0}
    rows = _query_unevaluated(username, **filters)
    if not rows:
        return {"evaluated": 0, "failed": 0, "remaining": 0}
    logger.info(
        "Starting evaluation: %d games, depth=%d, workers=%d, user=%s, filters=%s",
        len(rows), depth, workers, username, filters or "none",
    )
    return _run_eval_loop(username, rows, depth, progress_callback, stop_check, workers=workers)
