import asyncio
import json as _json
import logging
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

# Ensure backend dir is on sys.path when running from elsewhere
sys.path.insert(0, os.path.dirname(__file__))

from storage import (
    load_settings,
    save_settings,
    games_parquet_exists,
    query_games,
    query_game_by_id,
    query_last_date,
    query_unevaluated_count,
    query_evals_for_game,
    query_analytics,
    query_unique_openings,
    load_bookmarks,
    save_bookmarks,
    compute_game_eval_summaries,
    compute_mistake_patterns,
    query_reviews,
    get_unreviewed_games,
    query_review_by_game_id,
    upsert_review,
    get_reviews_path,
    mark_review_status,
)
from etl import parse_pgn_to_games, run_incremental_etl, run_full_etl

LICHESS_API = "https://lichess.org/api"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Give background runners a reference to this event loop for SSE streaming.
    from eval_runner import runner as eval_runner
    from review_runner import runner as review_runner
    loop = asyncio.get_event_loop()
    eval_runner.set_loop(loop)
    review_runner.set_loop(loop)

    # Auto-migrate on startup if parquet missing but old JSON exists
    try:
        from migrate import migrate_from_json
        from storage import games_parquet_exists as gpe
        settings = load_settings()
        default_user = settings.get("default_user", "luckleland")
        if not gpe(default_user):
            migrate_from_json(default_user)
    except Exception as e:
        print(f"[startup] Migration skipped or failed: {e}")
    yield


app = FastAPI(title="Lichess Analysis API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Models ────────────────────────────────────────────

class SettingsUpdate(BaseModel):
    default_user: str
    lichess_token: Optional[str] = None


class RefreshRequest(BaseModel):
    username: str


class EvalRequest(BaseModel):
    username: str
    depth: int = 15
    workers: Optional[int] = None  # None = auto-detect physical cores
    color: Optional[str] = None
    outcome: Optional[str] = None
    perf_type: Optional[str] = None
    since_date: Optional[str] = None
    until_date: Optional[str] = None
    bookmarked_only: bool = False
    opening: Optional[str] = None


# ── Settings ──────────────────────────────────────────

@app.post("/api/settings")
def update_settings(req: SettingsUpdate):
    settings = load_settings()
    settings["default_user"] = req.default_user
    if req.lichess_token is not None:
        settings["lichess_token"] = req.lichess_token
    save_settings(settings)
    # Never return the token to the client
    return {"default_user": settings["default_user"], "has_token": bool(settings.get("lichess_token"))}


@app.get("/api/settings")
def get_settings():
    s = load_settings()
    return {"default_user": s.get("default_user", "luckleland"), "has_token": bool(s.get("lichess_token"))}


# ── Status ────────────────────────────────────────────

@app.get("/api/status/{username}")
def get_status(username: str):
    has_data = games_parquet_exists(username)
    game_count = 0
    last_date = None
    unevaluated_count = 0

    if has_data:
        result = query_games(username, page=1, page_size=1)
        game_count = result.get("total", 0)
        last_date_obj = query_last_date(username)
        last_date = last_date_obj.isoformat() if last_date_obj else None
        unevaluated_count = query_unevaluated_count(username)

    return {
        "has_data": has_data,
        "game_count": game_count,
        "last_date": last_date,
        "unevaluated_count": unevaluated_count,
    }


@app.get("/api/last-date/{username}")
def get_last_date(username: str):
    d = query_last_date(username)
    return {"last_date": d.isoformat() if d else None}


# ── Games ─────────────────────────────────────────────

@app.get("/api/bookmarks/{username}")
def get_bookmarks(username: str):
    return load_bookmarks(username)


@app.post("/api/bookmarks/sync")
async def sync_bookmarks(req: RefreshRequest):
    settings = load_settings()
    token = settings.get("lichess_token", "").strip()
    if not token:
        raise HTTPException(status_code=400, detail="No Lichess API token configured. Add one in Settings.")

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/x-ndjson"}
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(
                f"{LICHESS_API}/games/export/bookmarks",
                params={"moves": "false", "tags": "true"},
                headers=headers,
            )
        if resp.status_code == 401:
            raise HTTPException(status_code=401, detail="Invalid Lichess API token.")
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=f"Lichess API error: {resp.text[:200]}")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Lichess API timed out")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Network error: {str(e)}")

    import json as _json
    game_ids = []
    for line in resp.text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = _json.loads(line)
            gid = obj.get("id")
            if gid:
                game_ids.append(gid)
        except Exception:
            pass

    save_bookmarks(req.username, game_ids)
    return {"synced": len(game_ids)}


@app.get("/api/games/{username}")
def get_games(
    username: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),
    color: Optional[str] = None,
    result: Optional[str] = None,
    outcome: Optional[str] = None,
    perf_type: Optional[str] = None,
    since_date: Optional[str] = None,
    until_date: Optional[str] = None,
    bookmarked_only: bool = False,
    opening: Optional[str] = None,
    evaluated_only: bool = False,
    termination: Optional[str] = None,
    min_moves: Optional[int] = None,
    max_moves: Optional[int] = None,
):
    return query_games(username, page, page_size, color, result, outcome, perf_type, since_date, until_date, bookmarked_only, opening, evaluated_only, termination, min_moves, max_moves)


@app.get("/api/game/{username}/{game_id}")
def get_game(username: str, game_id: str):
    game = query_game_by_id(username, game_id)
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    return game


@app.get("/api/evals/{username}/{game_id}")
def get_evals(username: str, game_id: str):
    return query_evals_for_game(username, game_id)


# ── Openings list ─────────────────────────────────────

@app.get("/api/openings/{username}")
def get_openings(username: str):
    return query_unique_openings(username)


# ── Analytics ─────────────────────────────────────────

@app.get("/api/analytics/{username}")
def get_analytics(
    username: str,
    since_date: Optional[str] = None,
    until_date: Optional[str] = None,
    color: Optional[str] = None,
    outcome: Optional[str] = None,
    perf_type: Optional[str] = None,
    opening: Optional[str] = None,
    termination: Optional[str] = None,
    min_moves: Optional[int] = None,
    max_moves: Optional[int] = None,
):
    return query_analytics(username, since_date, until_date, color, outcome, perf_type, opening, termination, min_moves, max_moves)


# ── Review Queue ──────────────────────────────────────

@app.get("/api/review/{username}")
def get_review_queue(
    username: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    color: Optional[str] = None,
    outcome: Optional[str] = None,
    perf_type: Optional[str] = None,
    since_date: Optional[str] = None,
    until_date: Optional[str] = None,
    opening: Optional[str] = None,
    sort_by: str = "blunder_count",
    termination: Optional[str] = None,
    min_moves: Optional[int] = None,
    max_moves: Optional[int] = None,
    bookmarked_only: bool = False,
):
    return compute_game_eval_summaries(
        username, page=page, page_size=page_size, color=color,
        outcome=outcome, perf_type=perf_type, since_date=since_date,
        until_date=until_date, opening=opening, sort_by=sort_by,
        termination=termination, min_moves=min_moves, max_moves=max_moves,
        bookmarked_only=bookmarked_only,
    )


@app.get("/api/mistake_patterns/{username}")
def get_mistake_patterns(
    username: str,
    color: Optional[str] = None,
    outcome: Optional[str] = None,
    perf_type: Optional[str] = None,
    since_date: Optional[str] = None,
    until_date: Optional[str] = None,
    opening: Optional[str] = None,
    termination: Optional[str] = None,
):
    return compute_mistake_patterns(
        username, color=color, outcome=outcome, perf_type=perf_type,
        since_date=since_date, until_date=until_date, opening=opening,
        termination=termination,
    )


# ── Reviews ───────────────────────────────────────────

class ReviewRequest(BaseModel):
    username: str
    color: Optional[str] = None
    outcome: Optional[str] = None
    perf_type: Optional[str] = None
    since_date: Optional[str] = None
    until_date: Optional[str] = None
    opening: Optional[str] = None
    termination: Optional[str] = None
    bookmarked_only: bool = False
    force: bool = False


@app.get("/api/reviews/{username}")
def get_reviews(
    username: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    sort_by: str = "blunder_count",
    sort_dir: str = "desc",
    color: Optional[str] = None,
    outcome: Optional[str] = None,
    perf_type: Optional[str] = None,
    since_date: Optional[str] = None,
    until_date: Optional[str] = None,
    opening: Optional[str] = None,
    termination: Optional[str] = None,
    min_moves: Optional[int] = None,
    max_moves: Optional[int] = None,
    bookmarked_only: bool = False,
    only_unreviewed: bool = True,
):
    return query_reviews(
        username, page=page, page_size=page_size,
        sort_by=sort_by, sort_dir=sort_dir,
        color=color, outcome=outcome, perf_type=perf_type,
        since_date=since_date, until_date=until_date, opening=opening,
        termination=termination, min_moves=min_moves, max_moves=max_moves,
        bookmarked_only=bookmarked_only, only_unreviewed=only_unreviewed,
    )


@app.get("/api/reviews/count/{username}")
def get_unreviewed_count(username: str):
    games = get_unreviewed_games(username)
    return {"count": len(games)}


@app.get("/api/reviews/status/{username}")
def get_review_status(username: str):
    from review_runner import runner as review_runner
    status = review_runner.get_status()
    try:
        status["unreviewed"] = len(get_unreviewed_games(username))
    except Exception:
        status["unreviewed"] = 0
    return status


@app.post("/api/reviews/generate")
def start_reviews(req: ReviewRequest):
    from review_runner import runner as review_runner
    filters = {
        k: v for k, v in {
            "color": req.color, "outcome": req.outcome, "perf_type": req.perf_type,
            "since_date": req.since_date, "until_date": req.until_date,
            "opening": req.opening, "termination": req.termination,
            "bookmarked_only": req.bookmarked_only,
        }.items() if v
    }
    started = review_runner.start(req.username, filters=filters, force=req.force)
    if not started:
        return {"started": False, "message": "Review generation already running"}
    return {"started": True}


@app.post("/api/reviews/stop")
def stop_reviews():
    from review_runner import runner as review_runner
    if not review_runner.is_running:
        return {"stopped": False, "message": "No review generation running"}
    review_runner.stop()
    return {"stopped": True}


@app.get("/api/reviews/stream/{username}")
async def stream_review_logs(username: str, request: Request):
    from review_runner import runner as review_runner

    async def generate():
        q = review_runner.subscribe()
        for line in list(review_runner.log_buffer):
            yield f"data: {_json.dumps({'log': line})}\n\n"
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    line = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield f"data: {_json.dumps({'log': line})}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            review_runner.unsubscribe(q)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/reviews/game/{username}/{game_id}")
def get_review_for_game(username: str, game_id: str):
    review = query_review_by_game_id(username, game_id)
    return review or {}


class MarkReviewRequest(BaseModel):
    is_reviewed: bool


@app.post("/api/reviews/game/{username}/{game_id}/mark")
def mark_review(username: str, game_id: str, req: MarkReviewRequest):
    found = mark_review_status(username, game_id, req.is_reviewed)
    if not found:
        raise HTTPException(status_code=404, detail="Review not found")
    return {"game_id": game_id, "is_reviewed": req.is_reviewed}


# ── Refresh ───────────────────────────────────────────

async def _fetch_pgn(username: str, since_ms: Optional[int] = None) -> str:
    params = {
        "opening": "true",
        "moves": "true",
    }
    if since_ms is not None:
        params["since"] = since_ms

    headers = {"Accept": "application/x-chess-pgn"}
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.get(
                f"{LICHESS_API}/games/user/{username}",
                params=params,
                headers=headers,
            )
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail=f"User '{username}' not found on Lichess")
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=f"Lichess API error: {resp.text[:200]}")
        return resp.text
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Lichess API timed out")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Network error: {str(e)}")


@app.post("/api/refresh/incremental")
async def refresh_incremental(req: RefreshRequest):
    last_date = query_last_date(req.username)
    since_ms = None
    if last_date:
        from datetime import datetime as dt
        since_ms = int(dt.combine(last_date, dt.min.time()).timestamp() * 1000)

    pgn_text = await _fetch_pgn(req.username, since_ms=since_ms)
    games = parse_pgn_to_games(pgn_text)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    result = run_incremental_etl(req.username, games, timestamp)
    result["fetched"] = len(games)
    return result


@app.post("/api/refresh/full")
async def refresh_full(req: RefreshRequest):
    # since Jan 1 2000
    since_ms = 946684800000
    pgn_text = await _fetch_pgn(req.username, since_ms=since_ms)
    games = parse_pgn_to_games(pgn_text)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    result = run_full_etl(req.username, games, timestamp)
    result["fetched"] = len(games)
    return result


# ── Evaluation ────────────────────────────────────────

@app.get("/api/evaluate/status/{username}")
def get_eval_status(username: str):
    from eval_runner import runner
    status = runner.get_status()
    status["unevaluated"] = query_unevaluated_count(username)
    return status


@app.get("/api/evaluate/count/{username}")
def get_eval_count(
    username: str,
    color: Optional[str] = None,
    outcome: Optional[str] = None,
    perf_type: Optional[str] = None,
    since_date: Optional[str] = None,
    until_date: Optional[str] = None,
    bookmarked_only: bool = False,
    opening: Optional[str] = None,
):
    from storage import query_unevaluated_count_filtered
    count = query_unevaluated_count_filtered(
        username, color=color, outcome=outcome, perf_type=perf_type,
        since_date=since_date, until_date=until_date,
        bookmarked_only=bookmarked_only, opening=opening,
    )
    return {"count": count}


@app.post("/api/evaluate")
async def start_evaluate(req: EvalRequest):
    from eval_runner import runner
    filters = {
        "color": req.color, "outcome": req.outcome, "perf_type": req.perf_type,
        "since_date": req.since_date, "until_date": req.until_date,
        "bookmarked_only": req.bookmarked_only, "opening": req.opening,
    }
    try:
        started = runner.start(req.username, req.depth, filters, workers=req.workers)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    if not started:
        return {"started": False, "message": "Evaluation already running"}
    return {"started": True}


@app.post("/api/evaluate/stop")
def stop_evaluate():
    from eval_runner import runner
    if not runner.is_running:
        return {"stopped": False, "message": "No evaluation running"}
    runner.stop()
    return {"stopped": True}


@app.get("/api/evaluate/stream/{username}")
async def stream_eval_logs(username: str, request: Request):
    from eval_runner import runner

    async def generate():
        q = runner.subscribe()
        # Replay full buffer so a returning client catches up
        for line in list(runner.log_buffer):
            yield f"data: {_json.dumps({'log': line})}\n\n"
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    line = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield f"data: {_json.dumps({'log': line})}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            runner.unsubscribe(q)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


class AnalyzePositionRequest(BaseModel):
    fen: str
    depth: int = 15
    moves: int = 6


@app.post("/api/analyze_position")
def analyze_position(req: AnalyzePositionRequest):
    import chess
    import chess.engine
    from evaluator import get_stockfish_path

    sf_path = get_stockfish_path()
    if not sf_path:
        raise HTTPException(status_code=503, detail="Stockfish not found")

    try:
        board = chess.Board(req.fen)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid FEN")

    engine = chess.engine.SimpleEngine.popen_uci(sf_path)
    try:
        info = engine.analyse(board, chess.engine.Limit(depth=min(req.depth, 20)))
        pv = info.get("pv", [])
        result = []
        b = board.copy()
        for move in pv[:req.moves]:
            san = b.san(move)
            b.push(move)
            result.append({"san": san, "uci": move.uci(), "fen": b.fen()})
        return {"pv": result}
    finally:
        engine.quit()


# ── SQL Editor ────────────────────────────────────────

class SqlQueryRequest(BaseModel):
    username: str
    sql: str


def _sql_duckdb_conn(username: str):
    """Create an in-memory DuckDB connection with games and evals registered as views."""
    import duckdb
    from storage import get_games_dir, get_evals_dir, games_parquet_exists

    con = duckdb.connect()
    games_dir = get_games_dir(username)
    evals_dir = get_evals_dir(username)

    if games_dir.exists() and any(games_dir.glob("*.parquet")):
        games_glob = str(games_dir / "*.parquet")
        con.execute(f"CREATE VIEW games AS SELECT * FROM read_parquet('{games_glob}')")

    if evals_dir.exists() and any(evals_dir.glob("*.parquet")):
        evals_glob = str(evals_dir / "*.parquet")
        con.execute(f"CREATE VIEW evals AS SELECT * FROM read_parquet('{evals_glob}')")

    reviews_path = get_reviews_path(username)
    if reviews_path.exists():
        con.execute(f"CREATE VIEW reviews AS SELECT * FROM read_parquet('{str(reviews_path)}')")

    return con


@app.get("/api/sql/schema/{username}")
def get_sql_schema(username: str):
    con = _sql_duckdb_conn(username)
    tables = {}
    try:
        views = con.execute("SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW'").fetchall()
        for (tname,) in views:
            cols = con.execute(f"DESCRIBE {tname}").fetchall()
            tables[tname] = [{"name": c[0], "type": c[1]} for c in cols]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()
    return tables


@app.post("/api/sql/query")
def run_sql_query(req: SqlQueryRequest):
    con = _sql_duckdb_conn(req.username)
    try:
        result = con.execute(req.sql)
        columns = [desc[0] for desc in result.description]
        rows = []
        for row in result.fetchall():
            serialized = {}
            for col, val in zip(columns, row):
                if hasattr(val, "isoformat"):
                    serialized[col] = val.isoformat()
                elif hasattr(val, "item"):
                    serialized[col] = val.item()
                else:
                    serialized[col] = val
            rows.append(serialized)
        return {"columns": columns, "rows": rows, "error": None}
    except Exception as e:
        return {"columns": [], "rows": [], "error": str(e)}
    finally:
        con.close()


# ── Health ────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}
