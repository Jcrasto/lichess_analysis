# Lichess Games Explorer

A local app for browsing your Lichess games, backed by the Lichess API.

## Project Structure

```
lichess-app/
├── backend/           # FastAPI server
│   ├── main.py
│   └── requirements.txt
├── frontend/          # React + Vite UI
│   ├── src/
│   │   ├── App.jsx
│   │   ├── components/
│   │   │   ├── GameList.jsx
│   │   │   ├── RefreshModal.jsx
│   │   │   └── GameDetail.jsx
│   └── package.json
└── exported_data/
    └── user_data/     # JSON files per user, written by the backend
```

## Setup & Running

### Backend (FastAPI)

```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

### Frontend (React + Vite)

```bash
cd frontend
npm install
npm run dev
```

Then open http://localhost:3000

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
      "headers": { "White": "...", "Black": "...", ... },
      "moves": "1. e4 e5 2. ..."
    }
  ]
}
```


# Commands
* uv run uvicorn main:app --reload --port 8000
* [ -s "/usr/local/opt/nvm/nvm.sh" ] && \. "/usr/local/opt/nvm/nvm.sh"
* npm install
* npm run dev