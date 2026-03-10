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
) -> dict:
    if not games_parquet_exists(username):
        return {"no_data": True}

    glob_path = str(get_games_dir(username) / "*.parquet")
    u = username.lower()
    conditions = []

    if color == "white":
        conditions.append(f"lower(white) = '{u}'")
    elif color == "black":
        conditions.append(f"lower(black) = '{u}'")

    # outcome is user-relative: win/loss/draw
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
        # raw PGN result fallback
        conditions.append(f"result = '{result}'")

    if perf_type:
        conditions.append(f"lower(perf_type) = lower('{perf_type}')")
    if since_date:
        conditions.append(f"date >= DATE '{since_date}'")
    if until_date:
        conditions.append(f"date <= DATE '{until_date}'")
    if bookmarked_only:
        bm_ids = load_bookmarks(username)
        if not bm_ids:
            return {"total": 0, "page": page, "page_size": page_size, "games": []}
        id_list = ", ".join(f"'{gid}'" for gid in bm_ids)
        conditions.append(f"game_id IN ({id_list})")

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
