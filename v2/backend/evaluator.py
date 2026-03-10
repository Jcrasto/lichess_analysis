import logging
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import chess
import chess.engine
import pandas as pd

logger = logging.getLogger("evaluator")


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


def evaluate_game(pgn: str, game_id: str, depth: int = 15) -> list:
    """Run Stockfish on a game PGN. Returns list of eval row dicts."""
    import chess.pgn
    import io

    sf_path = get_stockfish_path()
    if not sf_path:
        raise RuntimeError("Stockfish not found. Install via: brew install stockfish")

    pgn_io = io.StringIO(pgn)
    chess_game = chess.pgn.read_game(pgn_io)
    if chess_game is None:
        return []

    # Extract date/year/month from game headers
    date_obj = None
    date_str = chess_game.headers.get("Date", "")
    try:
        date_obj = datetime.strptime(date_str.replace(".??", "").replace("??", "01"), "%Y.%m.%d").date()
    except Exception:
        pass

    year = date_obj.year if date_obj else None
    month = date_obj.month if date_obj else None

    engine = chess.engine.SimpleEngine.popen_uci(sf_path)
    board = chess_game.board()
    evals = []
    now = datetime.now(timezone.utc)

    for move_number, move in enumerate(chess_game.mainline_moves(), start=1):
        board.push(move)
        fen = board.fen()
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
                "fen": fen,
                "cp_score": cp_score,
                "mate_in": mate_in,
                "best_move": best_move,
                "evaluated_at": now,
            }
        )

    engine.quit()
    return evals


def _evaluate_with_retry(pgn: str, game_id: str, depth: int, max_retries: int = 3) -> list:
    """Call evaluate_game with retries on failure."""
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
            time.sleep(2 ** (attempt - 1))  # 1s, 2s backoff
    if last_exc:
        raise last_exc
    return []


def run_evaluation_batch(username: str, batch_size: int = 50, depth: int = 15) -> dict:
    """Evaluate up to batch_size unevaluated games, newest first. Returns {evaluated, failed, remaining}."""
    if not games_parquet_exists(username):
        return {"evaluated": 0, "failed": 0, "remaining": 0}

    import duckdb

    glob_path = str(get_games_dir(username) / "*.parquet")
    con = duckdb.connect()
    rows = con.execute(
        f"""SELECT game_id, pgn, year, month FROM read_parquet('{glob_path}')
            WHERE has_eval = false
            ORDER BY date DESC
            LIMIT {batch_size}"""
    ).fetchall()
    con.close()

    if not rows:
        return {"evaluated": 0, "failed": 0, "remaining": 0}

    sf_path = get_stockfish_path()
    if not sf_path:
        raise RuntimeError("Stockfish not found")

    total_queued = len(rows)
    logger.info("Starting evaluation batch: %d games, depth=%d, user=%s (newest first)", total_queued, depth, username)

    evaluated = 0
    failed = 0
    for i, (game_id, pgn, year, month) in enumerate(rows, start=1):
        try:
            logger.info("[%d/%d] Evaluating %s (%d-%02d)", i, total_queued, game_id, int(year), int(month))
            eval_rows = _evaluate_with_retry(pgn, game_id, depth=depth)
            if not eval_rows:
                logger.warning("[%d/%d] Skipping %s — no plies after retries", i, total_queued, game_id)
                failed += 1
                continue

            # Write evals to parquet
            df_evals = _normalize_evals_df(pd.DataFrame(eval_rows))
            existing_evals = load_eval_month_parquet(username, int(year), int(month))
            if existing_evals.empty:
                combined_evals = df_evals
            else:
                combined_evals = pd.concat(
                    [_normalize_evals_df(existing_evals), df_evals], ignore_index=True
                ).drop_duplicates(subset=["game_id", "move_number"], keep="last")
            write_eval_month_parquet(username, int(year), int(month), combined_evals.reset_index(drop=True))

            # Update has_eval in games parquet
            df_games = load_month_parquet(username, int(year), int(month))
            if not df_games.empty:
                df_games.loc[df_games["game_id"] == game_id, "has_eval"] = True
                write_month_parquet(username, int(year), int(month), df_games)

            evaluated += 1
            logger.info("[%d/%d] Done: %s — %d plies evaluated", i, total_queued, game_id, len(eval_rows))
        except Exception as e:
            failed += 1
            logger.error("[%d/%d] Permanently failed %s: %s", i, total_queued, game_id, e, exc_info=True)

    remaining = query_unevaluated_count(username)
    logger.info("Batch complete: evaluated=%d, failed=%d, remaining=%d", evaluated, failed, remaining)
    return {"evaluated": evaluated, "failed": failed, "remaining": remaining}
