import os
import json
import re
import httpx
from pathlib import Path
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="Lichess Games API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_DIR = Path(__file__).parent.parent / "exported_data" / "user_data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

LICHESS_API = "https://lichess.org/api"


class RefreshRequest(BaseModel):
    username: str
    since: Optional[str] = None  # ISO date string
    until: Optional[str] = None  # ISO date string
    max: Optional[int] = 100
    perf_type: Optional[str] = None  # bullet, blitz, rapid, classical, etc.


def parse_pgn_games(pgn_text: str) -> list[dict]:
    """Parse PGN text into a list of game dicts."""
    games = []
    # Split on double newlines before [Event
    raw_games = re.split(r'\n\n(?=\[Event)', pgn_text.strip())

    for raw in raw_games:
        if not raw.strip():
            continue
        game = {"pgn": raw.strip(), "headers": {}, "moves": ""}
        lines = raw.strip().split("\n")
        move_lines = []
        in_moves = False
        for line in lines:
            if line.startswith("[") and not in_moves:
                m = re.match(r'\[(\w+)\s+"(.*)"\]', line)
                if m:
                    game["headers"][m.group(1)] = m.group(2)
            else:
                in_moves = True
                if line.strip():
                    move_lines.append(line)
        game["moves"] = " ".join(move_lines)
        if game["headers"]:
            games.append(game)
    return games


def get_user_file(username: str) -> Path:
    return DATA_DIR / f"{username.lower()}.json"


def load_user_data(username: str) -> dict:
    f = get_user_file(username)
    if f.exists():
        with open(f) as fp:
            return json.load(fp)
    return {"username": username, "games": [], "last_updated": None}


def save_user_data(username: str, data: dict):
    f = get_user_file(username)
    data["last_updated"] = datetime.utcnow().isoformat()
    with open(f, "w") as fp:
        json.dump(data, fp, indent=2)


@app.get("/api/users")
def list_users():
    """List all users we have local data for."""
    users = []
    for f in DATA_DIR.glob("*.json"):
        with open(f) as fp:
            d = json.load(fp)
        users.append({
            "username": d.get("username"),
            "game_count": len(d.get("games", [])),
            "last_updated": d.get("last_updated"),
        })
    return users


@app.get("/api/games/{username}")
def get_games(
    username: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    color: Optional[str] = None,
    result: Optional[str] = None,
    perf_type: Optional[str] = None,
):
    """Return paginated local game data for a user."""
    data = load_user_data(username)
    games = data.get("games", [])

    # Filters
    if color:
        games = [g for g in games if g.get("headers", {}).get("White", "").lower() == username.lower() and color == "white"
                 or g.get("headers", {}).get("Black", "").lower() == username.lower() and color == "black"]
    if result:
        games = [g for g in games if g.get("headers", {}).get("Result") == result]
    if perf_type:
        games = [g for g in games if g.get("headers", {}).get("Event", "").lower().find(perf_type.lower()) != -1]

    total = len(games)
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "username": username,
        "total": total,
        "page": page,
        "page_size": page_size,
        "last_updated": data.get("last_updated"),
        "games": games[start:end],
    }


@app.post("/api/refresh")
async def refresh_user_data(req: RefreshRequest):
    """Fetch games from Lichess API and write to exported_data/user_data."""
    params = {
        "max": req.max,
        "evals": "true",
        "opening": "true",
        "clocks": "true",
        "moves": "true",
    }
    if req.since:
        dt = datetime.fromisoformat(req.since)
        params["since"] = int(dt.timestamp() * 1000)
    if req.until:
        dt = datetime.fromisoformat(req.until)
        params["until"] = int(dt.timestamp() * 1000)
    if req.perf_type:
        params["perfType"] = req.perf_type

    headers = {"Accept": "application/x-chess-pgn"}

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(
                f"{LICHESS_API}/games/user/{req.username}",
                params=params,
                headers=headers,
            )
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail=f"User '{req.username}' not found on Lichess")
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=f"Lichess API error: {resp.text[:200]}")

        pgn_text = resp.text
        new_games = parse_pgn_games(pgn_text)

        # Merge with existing data (dedupe by GameId header)
        existing = load_user_data(req.username)
        existing_ids = {g["headers"].get("Site", "") for g in existing["games"]}
        added = 0
        for g in new_games:
            site = g["headers"].get("Site", "")
            if site not in existing_ids:
                existing["games"].append(g)
                existing_ids.add(site)
                added += 1

        # Sort by date descending
        existing["games"].sort(
            key=lambda g: g["headers"].get("Date", ""), reverse=True
        )
        existing["username"] = req.username
        save_user_data(req.username, existing)

        return {
            "success": True,
            "fetched": len(new_games),
            "added": added,
            "total": len(existing["games"]),
        }

    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Lichess API timed out")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Network error: {str(e)}")


@app.get("/api/game/{username}/{game_id}")
def get_game_detail(username: str, game_id: str):
    """Get a single game by its Lichess ID."""
    data = load_user_data(username)
    for g in data["games"]:
        site = g["headers"].get("Site", "")
        if game_id in site:
            return g
    raise HTTPException(status_code=404, detail="Game not found")


@app.get("/health")
def health():
    return {"status": "ok"}
