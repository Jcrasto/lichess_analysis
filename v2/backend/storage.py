import json
import duckdb
import pandas as pd
from pathlib import Path
from datetime import date
from typing import Optional

BASE_DIR = Path(__file__).parent.parent / "exported_data"
SETTINGS_FILE = BASE_DIR / "settings.json"


def get_raw_dir(username: str) -> Path:
    return BASE_DIR / username / "raw"


def get_games_dir(username: str) -> Path:
    return BASE_DIR / username / "processed" / "games"


def get_bookmarks_file(username: str) -> Path:
    return BASE_DIR / username / "bookmarks.json"


def load_bookmarks(username: str) -> list:
    f = get_bookmarks_file(username)
    if f.exists():
        with open(f) as fp:
            return json.load(fp)
    return []


def save_bookmarks(username: str, game_ids: list):
    f = get_bookmarks_file(username)
    f.parent.mkdir(parents=True, exist_ok=True)
    with open(f, "w") as fp:
        json.dump(game_ids, fp)


def get_evals_dir(username: str) -> Path:
    return BASE_DIR / username / "processed" / "evals"


def load_settings() -> dict:
    if SETTINGS_FILE.exists():
        with open(SETTINGS_FILE) as f:
            return json.load(f)
    return {"default_user": "luckleland"}


def save_settings(settings: dict):
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    with open(SETTINGS_FILE, "w") as f:
        json.dump(settings, f, indent=2)


def games_parquet_exists(username: str) -> bool:
    d = get_games_dir(username)
    return d.exists() and any(d.glob("*.parquet"))


def _serialize_row(row: dict) -> dict:
    """Convert non-JSON-serializable values in a row dict."""
    for k, v in row.items():
        if hasattr(v, "isoformat"):
            row[k] = v.isoformat()
        elif hasattr(v, "item"):  # numpy scalar
            row[k] = v.item()
    return row


def _build_game_conditions(
    username: str,
    color: Optional[str] = None,
    result: Optional[str] = None,
    outcome: Optional[str] = None,
    perf_type: Optional[str] = None,
    since_date: Optional[str] = None,
    until_date: Optional[str] = None,
    bookmarked_only: bool = False,
    opening: Optional[str] = None,
    evaluated_only: bool = False,
) -> list:
    """Build WHERE conditions list shared by query_games and eval queries."""
    u = username.lower()
    conditions = []

    if color == "white":
        conditions.append(f"lower(white) = '{u}'")
    elif color == "black":
        conditions.append(f"lower(black) = '{u}'")

    if outcome == "win":
        conditions.append(
            f"((lower(white) = '{u}' AND result = '1-0') OR (lower(black) = '{u}' AND result = '0-1'))"
        )
    elif outcome == "loss":
        conditions.append(
            f"((lower(white) = '{u}' AND result = '0-1') OR (lower(black) = '{u}' AND result = '1-0'))"
        )
    elif outcome == "draw":
        conditions.append("result = '1/2-1/2'")
    elif result:
        conditions.append(f"result = '{result}'")

    if perf_type:
        conditions.append(f"lower(perf_type) = lower('{perf_type}')")
    if since_date:
        conditions.append(f"date >= DATE '{since_date}'")
    if until_date:
        conditions.append(f"date <= DATE '{until_date}'")
    if bookmarked_only:
        bm_ids = load_bookmarks(username)
        id_list = ", ".join(f"'{gid}'" for gid in bm_ids) if bm_ids else None
        conditions.append(f"game_id IN ({id_list})" if id_list else "1 = 0")
    if opening:
        safe = opening.replace("'", "''")
        conditions.append(f"lower(opening) = lower('{safe}')")   # exact match
    if evaluated_only:
        conditions.append("has_eval = true")

    return conditions


def query_games(
    username: str,
    page: int = 1,
    page_size: int = 100,
    color: Optional[str] = None,
    result: Optional[str] = None,
    outcome: Optional[str] = None,
    perf_type: Optional[str] = None,
    since_date: Optional[str] = None,
    until_date: Optional[str] = None,
    bookmarked_only: bool = False,
    opening: Optional[str] = None,
    evaluated_only: bool = False,
) -> dict:
    if not games_parquet_exists(username):
        return {"no_data": True}

    glob_path = str(get_games_dir(username) / "*.parquet")
    conditions = _build_game_conditions(username, color, result, outcome, perf_type, since_date, until_date, bookmarked_only, opening, evaluated_only)
    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    offset = (page - 1) * page_size

    con = duckdb.connect()
    total = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{glob_path}') {where_clause}"
    ).fetchone()[0]

    rows = con.execute(
        f"""SELECT * FROM read_parquet('{glob_path}')
            {where_clause}
            ORDER BY date DESC
            LIMIT {page_size} OFFSET {offset}"""
    ).fetchall()
    cols = [d[0] for d in con.description]
    con.close()

    games = [_serialize_row(dict(zip(cols, row))) for row in rows]
    return {"total": total, "page": page, "page_size": page_size, "games": games}


def query_unevaluated_games(
    username: str,
    color: Optional[str] = None,
    result: Optional[str] = None,
    outcome: Optional[str] = None,
    perf_type: Optional[str] = None,
    since_date: Optional[str] = None,
    until_date: Optional[str] = None,
    bookmarked_only: bool = False,
    opening: Optional[str] = None,
) -> list:
    """Return (game_id, pgn, year, month, date) rows for unevaluated games matching filters."""
    if not games_parquet_exists(username):
        return []
    import duckdb as _duckdb
    glob_path = str(get_games_dir(username) / "*.parquet")
    conditions = _build_game_conditions(username, color, result, outcome, perf_type, since_date, until_date, bookmarked_only, opening)
    conditions.append("(has_eval = false OR has_eval IS NULL)")
    where_clause = "WHERE " + " AND ".join(conditions)
    con = _duckdb.connect()
    rows = con.execute(
        f"SELECT game_id, pgn, year, month, date FROM read_parquet('{glob_path}') {where_clause} ORDER BY date DESC"
    ).fetchall()
    con.close()
    return rows


def query_unevaluated_count_filtered(
    username: str,
    color: Optional[str] = None,
    result: Optional[str] = None,
    outcome: Optional[str] = None,
    perf_type: Optional[str] = None,
    since_date: Optional[str] = None,
    until_date: Optional[str] = None,
    bookmarked_only: bool = False,
    opening: Optional[str] = None,
) -> int:
    if not games_parquet_exists(username):
        return 0
    import duckdb as _duckdb
    glob_path = str(get_games_dir(username) / "*.parquet")
    conditions = _build_game_conditions(username, color, result, outcome, perf_type, since_date, until_date, bookmarked_only, opening)
    conditions.append("(has_eval = false OR has_eval IS NULL)")
    where_clause = "WHERE " + " AND ".join(conditions)
    con = _duckdb.connect()
    count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{glob_path}') {where_clause}"
    ).fetchone()[0]
    con.close()
    return count


def query_game_by_id(username: str, game_id: str) -> Optional[dict]:
    if not games_parquet_exists(username):
        return None
    glob_path = str(get_games_dir(username) / "*.parquet")
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT * FROM read_parquet('{glob_path}') WHERE game_id = ?", [game_id]
    ).fetchall()
    if not rows:
        con.close()
        return None
    cols = [d[0] for d in con.description]
    con.close()
    return _serialize_row(dict(zip(cols, rows[0])))


def query_last_date(username: str) -> Optional[date]:
    if not games_parquet_exists(username):
        return None
    glob_path = str(get_games_dir(username) / "*.parquet")
    con = duckdb.connect()
    row = con.execute(
        f"SELECT MAX(date) FROM read_parquet('{glob_path}')"
    ).fetchone()
    con.close()
    if row and row[0]:
        return row[0]
    return None


def query_unevaluated_count(username: str) -> int:
    if not games_parquet_exists(username):
        return 0
    glob_path = str(get_games_dir(username) / "*.parquet")
    con = duckdb.connect()
    row = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{glob_path}') WHERE has_eval = false"
    ).fetchone()
    con.close()
    return row[0] if row else 0


def load_month_parquet(username: str, year: int, month: int) -> pd.DataFrame:
    path = get_games_dir(username) / f"{year:04d}_{month:02d}.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


def write_month_parquet(username: str, year: int, month: int, df: pd.DataFrame):
    path = get_games_dir(username) / f"{year:04d}_{month:02d}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def load_eval_month_parquet(username: str, year: int, month: int) -> pd.DataFrame:
    path = get_evals_dir(username) / f"{year:04d}_{month:02d}.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


def write_eval_month_parquet(username: str, year: int, month: int, df: pd.DataFrame):
    path = get_evals_dir(username) / f"{year:04d}_{month:02d}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def query_evals_for_game(username: str, game_id: str) -> list:
    evals_dir = get_evals_dir(username)
    if not evals_dir.exists() or not any(evals_dir.glob("*.parquet")):
        return []
    glob_path = str(evals_dir / "*.parquet")
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT * FROM read_parquet('{glob_path}') WHERE game_id = ? ORDER BY move_number",
        [game_id],
    ).fetchall()
    if not rows:
        con.close()
        return []
    cols = [d[0] for d in con.description]
    con.close()
    return [_serialize_row(dict(zip(cols, row))) for row in rows]


def query_unique_openings(username: str) -> list:
    """Return all unique opening names, sorted by frequency descending."""
    if not games_parquet_exists(username):
        return []
    glob_path = str(get_games_dir(username) / "*.parquet")
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT opening
        FROM read_parquet('{glob_path}')
        WHERE opening IS NOT NULL AND TRIM(opening) != '' AND opening != '?'
        GROUP BY opening
        ORDER BY COUNT(*) DESC, opening
    """).fetchall()
    con.close()
    return [r[0] for r in rows]


def query_analytics(
    username: str,
    since_date: Optional[str] = None,
    until_date: Optional[str] = None,
) -> dict:
    if not games_parquet_exists(username):
        return {}

    glob_path = str(get_games_dir(username) / "*.parquet")
    u = username.lower()

    # Build reusable date filter (always a valid boolean expression)
    date_conds = []
    if since_date:
        date_conds.append(f"date >= DATE '{since_date}'")
    if until_date:
        date_conds.append(f"date <= DATE '{until_date}'")
    date_filter = " AND ".join(date_conds) if date_conds else "TRUE"

    win_expr  = f"((lower(white)='{u}' AND result='1-0') OR (lower(black)='{u}' AND result='0-1'))"
    loss_expr = f"((lower(white)='{u}' AND result='0-1') OR (lower(black)='{u}' AND result='1-0'))"
    draw_expr = "result='1/2-1/2'"

    con = duckdb.connect()

    # Full data date range (unfiltered — used to set slider bounds)
    dr = con.execute(
        f"SELECT MIN(date), MAX(date) FROM read_parquet('{glob_path}')"
    ).fetchone()
    date_range = {
        "min": dr[0].isoformat() if dr[0] else None,
        "max": dr[1].isoformat() if dr[1] else None,
    }

    # Games per month (wins / losses / draws)
    rows = con.execute(f"""
        SELECT year, month,
            COUNT(*) AS count,
            SUM(CASE WHEN {win_expr}  THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN {loss_expr} THEN 1 ELSE 0 END) AS losses,
            SUM(CASE WHEN {draw_expr} THEN 1 ELSE 0 END) AS draws
        FROM read_parquet('{glob_path}')
        WHERE {date_filter}
        GROUP BY year, month ORDER BY year, month
    """).fetchall()
    games_per_month = [
        {"period": f"{r[0]}-{r[1]:02d}", "count": r[2], "wins": r[3], "losses": r[4], "draws": r[5]}
        for r in rows
    ]

    # Average ELO per month
    rows = con.execute(f"""
        SELECT year, month,
            ROUND(AVG(CASE WHEN lower(white)='{u}' THEN CAST(white_elo AS DOUBLE)
                          WHEN lower(black)='{u}' THEN CAST(black_elo AS DOUBLE)
                     END)) AS avg_elo
        FROM read_parquet('{glob_path}')
        WHERE {date_filter}
          AND (lower(white)='{u}' OR lower(black)='{u}')
        GROUP BY year, month ORDER BY year, month
    """).fetchall()
    elo_per_month = [
        {"period": f"{r[0]}-{r[1]:02d}", "elo": int(r[2])}
        for r in rows if r[2] is not None
    ]

    # Top openings (stacked wins/losses/draws)
    rows = con.execute(f"""
        SELECT opening, COUNT(*) AS count,
            SUM(CASE WHEN {win_expr}  THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN {draw_expr} THEN 1 ELSE 0 END) AS draws,
            SUM(CASE WHEN {loss_expr} THEN 1 ELSE 0 END) AS losses
        FROM read_parquet('{glob_path}')
        WHERE ({date_filter})
          AND opening IS NOT NULL AND TRIM(opening) != '' AND opening != '?'
        GROUP BY opening ORDER BY count DESC LIMIT 15
    """).fetchall()
    top_openings = [
        {"opening": r[0], "count": r[1], "wins": r[2], "draws": r[3], "losses": r[4]}
        for r in rows
    ]

    # Overall result summary
    r = con.execute(f"""
        SELECT
            SUM(CASE WHEN {win_expr}  THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN {loss_expr} THEN 1 ELSE 0 END) AS losses,
            SUM(CASE WHEN {draw_expr} THEN 1 ELSE 0 END) AS draws,
            COUNT(*) AS total
        FROM read_parquet('{glob_path}')
        WHERE {date_filter}
    """).fetchone()
    result_summary = {"wins": r[0], "losses": r[1], "draws": r[2], "total": r[3]}

    # Performance type breakdown
    rows = con.execute(f"""
        SELECT perf_type, COUNT(*) AS count,
            SUM(CASE WHEN {win_expr}  THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN {loss_expr} THEN 1 ELSE 0 END) AS losses,
            SUM(CASE WHEN {draw_expr} THEN 1 ELSE 0 END) AS draws
        FROM read_parquet('{glob_path}')
        WHERE ({date_filter})
          AND perf_type IS NOT NULL AND perf_type != ''
        GROUP BY perf_type ORDER BY count DESC
    """).fetchall()
    perf_breakdown = [
        {"perf_type": r[0], "count": r[1], "wins": r[2], "losses": r[3], "draws": r[4]}
        for r in rows
    ]

    con.close()
    return {
        "date_range": date_range,
        "games_per_month": games_per_month,
        "elo_per_month": elo_per_month,
        "top_openings": top_openings,
        "result_summary": result_summary,
        "perf_breakdown": perf_breakdown,
    }


def update_has_eval(username: str, game_id: str, value: bool = True):
    """Update has_eval flag in the month parquet for a game."""
    if not games_parquet_exists(username):
        return
    glob_path = str(get_games_dir(username) / "*.parquet")
    con = duckdb.connect()
    row = con.execute(
        f"SELECT year, month FROM read_parquet('{glob_path}') WHERE game_id = ?",
        [game_id],
    ).fetchone()
    con.close()
    if not row:
        return
    year, month = int(row[0]), int(row[1])
    df = load_month_parquet(username, year, month)
    if df.empty:
        return
    df.loc[df["game_id"] == game_id, "has_eval"] = value
    write_month_parquet(username, year, month, df)
