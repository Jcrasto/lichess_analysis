# Lichess Games Explorer

A local app for browsing your Lichess games, backed by the Lichess API.

## Project Structure

```
v2/
├── backend/                  # FastAPI server (Python)
│   ├── main.py
│   └── pyproject.toml
├── frontend/                 # React + Vite UI
│   ├── index.html
│   ├── vite.config.js
│   ├── package.json
│   └── src/
│       ├── main.jsx
│       ├── App.jsx
│       ├── App.css
│       └── components/
│           ├── GameList.jsx
│           ├── RefreshModal.jsx
│           └── GameDetail.jsx
└── exported_data/
    └── user_data/            # JSON files per user, written by the backend
```

## Setup & Running

### Prerequisites

- **Python**: [uv](https://docs.astral.sh/uv/) (manages the venv automatically)
- **Node**: v18+ with npm

---

### Backend (FastAPI)

```bash
cd backend
uv run uvicorn main:app --reload --port 8000
```

Runs at `http://localhost:8000`. The `--reload` flag auto-restarts on file changes.

> First run: `uv` will create a `.venv` and install dependencies from `pyproject.toml` automatically.

---

### Frontend (React + Vite)

```bash
cd frontend
npm install       # first time only
npm run dev
```

Runs at `http://localhost:3000`. API calls to `/api/*` are proxied to the backend automatically.

---

## How it works

1. Enter a Lichess username and click **LOAD** to view locally cached games.
2. Click **REFRESH USER DATA** to open the refresh modal.
3. Configure the username, date range, max games, and game type.
4. Click **FETCH GAMES** — the backend calls `https://lichess.org/api/games/user/{username}` and writes the results to `exported_data/user_data/{username}.json`.
5. New games are merged with existing data (deduplicated by Lichess game URL).

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/users` | List all locally cached users |
| GET | `/api/games/{username}` | Paginated games for a user |
| POST | `/api/refresh` | Fetch & store games from Lichess |
| GET | `/api/game/{username}/{id}` | Single game detail |
| GET | `/health` | Health check |

## Data Storage

Each user's games are stored in `exported_data/user_data/{username}.json`:

```json
{
  "username": "...",
  "last_updated": "...",
  "games": [
    {
      "pgn": "...",
      "headers": { "White": "...", "Black": "...", "Result": "...", "Date": "..." },
      "moves": "1. e4 e5 2. ..."
    }
  ]
}
```
