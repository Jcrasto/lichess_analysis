import json
from pathlib import Path
from datetime import datetime

from storage import games_parquet_exists
from etl import run_full_etl, infer_perf_type, _parse_elo

OLD_DATA_DIR = Path(__file__).parent.parent / "exported_data" / "user_data"


def migrate_from_json(username: str = "luckleland"):
    """Migrate old nested JSON format to partitioned Parquet."""
    old_file = OLD_DATA_DIR / f"{username.lower()}.json"
    if not old_file.exists():
        print(f"[migrate] No old data file found for {username}")
        return

    print(f"[migrate] Migrating {username} from {old_file}...")
    with open(old_file) as f:
        data = json.load(f)

    old_games = data.get("games", [])
    print(f"[migrate] Found {len(old_games)} games in old format")

    flat_games = []
    for og in old_games:
        h = og.get("headers", {})
        moves = og.get("moves", "")
        pgn = og.get("pgn", "")

        site = h.get("Site", "")
        game_id = site.rsplit("/", 1)[-1] if site else ""

        date_str = h.get("Date", "").replace(".??", "").replace("??", "01")
        try:
            parsed_date = datetime.strptime(date_str, "%Y.%m.%d").date()
        except Exception:
            parsed_date = None

        event = h.get("Event", "")
        flat_games.append(
            {
                "game_id": game_id,
                "date": parsed_date,
                "year": parsed_date.year if parsed_date else None,
                "month": parsed_date.month if parsed_date else None,
                "white": h.get("White", ""),
                "black": h.get("Black", ""),
                "white_elo": _parse_elo(h.get("WhiteElo")),
                "black_elo": _parse_elo(h.get("BlackElo")),
                "result": h.get("Result", ""),
                "eco": h.get("ECO"),
                "opening": h.get("Opening"),
                "time_control": h.get("TimeControl", ""),
                "termination": h.get("Termination", ""),
                "event": event,
                "perf_type": infer_perf_type(event),
                "pgn": pgn,
                "moves": moves,
                "has_eval": False,
            }
        )

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    result = run_full_etl(username, flat_games, timestamp)
    print(f"[migrate] Done: {result}")
    return result


if __name__ == "__main__":
    migrate_from_json()
