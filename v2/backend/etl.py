import json
import re
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional

from storage import get_raw_dir, get_games_dir, load_month_parquet, write_month_parquet


def infer_perf_type(event_str: str) -> str:
    if not event_str:
        return "unknown"
    low = event_str.lower()
    for k in ["bullet", "blitz", "rapid", "classical", "correspondence", "puzzle"]:
        if k in low:
            return k
    return "unknown"


def _parse_elo(val) -> Optional[int]:
    if not val or val == "?":
        return None
    try:
        return int(val)
    except Exception:
        return None


def parse_pgn_to_games(pgn_text: str) -> list:
    """Parse PGN text into list of flat game dicts."""
    games = []
    raw_games = re.split(r"\n\n(?=\[Event)", pgn_text.strip())

    for raw in raw_games:
        if not raw.strip():
            continue
        headers = {}
        lines = raw.strip().split("\n")
        move_lines = []
        in_moves = False
        for line in lines:
            if line.startswith("[") and not in_moves:
                m = re.match(r'\[(\w+)\s+"(.*)"\]', line)
                if m:
                    headers[m.group(1)] = m.group(2)
            else:
                in_moves = True
                if line.strip():
                    move_lines.append(line)

        if not headers:
            continue

        moves = " ".join(move_lines)
        site = headers.get("Site", "")
        game_id = site.rsplit("/", 1)[-1] if site else ""

        date_str = headers.get("Date", "").replace(".??", "").replace("??", "01")
        try:
            parsed_date = datetime.strptime(date_str, "%Y.%m.%d").date()
        except Exception:
            parsed_date = None

        event = headers.get("Event", "")
        games.append(
            {
                "game_id": game_id,
                "date": parsed_date,
                "year": parsed_date.year if parsed_date else None,
                "month": parsed_date.month if parsed_date else None,
                "white": headers.get("White", ""),
                "black": headers.get("Black", ""),
                "white_elo": _parse_elo(headers.get("WhiteElo")),
                "black_elo": _parse_elo(headers.get("BlackElo")),
                "result": headers.get("Result", ""),
                "eco": headers.get("ECO"),
                "opening": headers.get("Opening"),
                "time_control": headers.get("TimeControl", ""),
                "termination": headers.get("Termination", ""),
                "event": event,
                "perf_type": infer_perf_type(event),
                "pgn": raw.strip(),
                "moves": moves,
                "has_eval": False,
            }
        )
    return games


def games_to_dataframe(games: list) -> pd.DataFrame:
    if not games:
        return pd.DataFrame()
    df = pd.DataFrame(games)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    for col in ["year", "month"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")
    for col in ["white_elo", "black_elo"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")
    if "has_eval" in df.columns:
        df["has_eval"] = df["has_eval"].astype(bool)
    return df


def _json_serializable(games: list) -> list:
    """Return games list with date objects converted to ISO strings."""
    result = []
    for g in games:
        row = dict(g)
        if hasattr(row.get("date"), "isoformat"):
            row["date"] = row["date"].isoformat()
        result.append(row)
    return result


def run_incremental_etl(username: str, games: list, timestamp: str) -> dict:
    """Write raw JSON, then update only affected month parquets."""
    raw_dir = get_raw_dir(username)
    raw_dir.mkdir(parents=True, exist_ok=True)
    with open(raw_dir / f"games_{timestamp}.json", "w") as f:
        json.dump(_json_serializable(games), f)

    if not games:
        return {"months_updated": 0, "new_games": 0, "total_games": 0}

    df_new = games_to_dataframe(games)
    df_new = df_new.dropna(subset=["year", "month"])

    months = df_new.groupby(["year", "month"]).size().index.tolist()

    total_new = 0
    for year, month in months:
        year, month = int(year), int(month)
        existing = load_month_parquet(username, year, month)
        month_df = df_new[(df_new["year"] == year) & (df_new["month"] == month)]

        if existing.empty:
            combined = month_df
            new_count = len(combined)
        else:
            before = len(existing)
            combined = pd.concat([existing, month_df])
            combined = combined.drop_duplicates(subset=["game_id"], keep="first")
            new_count = len(combined) - before

        total_new += max(new_count, 0)
        write_month_parquet(username, year, month, combined.reset_index(drop=True))

    total = _count_total_games(username)
    return {"months_updated": len(months), "new_games": total_new, "total_games": total}


def run_full_etl(username: str, games: list, timestamp: str) -> dict:
    """Write raw JSON, clear all parquets, write fresh by month."""
    raw_dir = get_raw_dir(username)
    raw_dir.mkdir(parents=True, exist_ok=True)
    with open(raw_dir / f"games_{timestamp}.json", "w") as f:
        json.dump(_json_serializable(games), f)

    games_dir = get_games_dir(username)
    if games_dir.exists():
        for p in games_dir.glob("*.parquet"):
            p.unlink()

    if not games:
        return {"months_written": 0, "total_games": 0}

    df = games_to_dataframe(games)
    df = df.dropna(subset=["year", "month"])

    months_written = 0
    for (year, month), group in df.groupby(["year", "month"]):
        write_month_parquet(username, int(year), int(month), group.reset_index(drop=True))
        months_written += 1

    return {"months_written": months_written, "total_games": len(df)}


def _count_total_games(username: str) -> int:
    games_dir = get_games_dir(username)
    if not games_dir.exists():
        return 0
    total = 0
    for p in games_dir.glob("*.parquet"):
        df = pd.read_parquet(p, columns=["game_id"])
        total += len(df)
    return total
