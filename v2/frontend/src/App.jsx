import { useState, useEffect, useCallback } from 'react'
import GameList from './components/GameList.jsx'
import RefreshModal from './components/RefreshModal.jsx'
import GameDetail from './components/GameDetail.jsx'
import './App.css'

export default function App() {
  const [username, setUsername] = useState('')
  const [inputUser, setInputUser] = useState('')
  const [games, setGames] = useState([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [lastUpdated, setLastUpdated] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [showRefresh, setShowRefresh] = useState(false)
  const [selectedGame, setSelectedGame] = useState(null)
  const [filters, setFilters] = useState({ color: '', result: '', perf_type: '' })
  const [users, setUsers] = useState([])

  const PAGE_SIZE = 20

  const fetchUsers = useCallback(async () => {
    try {
      const r = await fetch('/api/users')
      const d = await r.json()
      setUsers(d)
    } catch {}
  }, [])

  useEffect(() => { fetchUsers() }, [fetchUsers])

  const fetchGames = useCallback(async (user, p = 1, f = filters) => {
    if (!user) return
    setLoading(true)
    setError(null)
    try {
      const params = new URLSearchParams({
        page: p,
        page_size: PAGE_SIZE,
        ...(f.color && { color: f.color }),
        ...(f.result && { result: f.result }),
        ...(f.perf_type && { perf_type: f.perf_type }),
      })
      const r = await fetch(`/api/games/${user}?${params}`)
      if (!r.ok) {
        const err = await r.json()
        throw new Error(err.detail || 'Failed to load games')
      }
      const d = await r.json()
      setGames(d.games)
      setTotal(d.total)
      setLastUpdated(d.last_updated)
      setPage(p)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [filters])

  const handleLoad = () => {
    if (!inputUser.trim()) return
    setUsername(inputUser.trim())
    setPage(1)
    setGames([])
    fetchGames(inputUser.trim(), 1, filters)
  }

  const handleRefreshDone = (result) => {
    setShowRefresh(false)
    fetchUsers()
    if (username) fetchGames(username, 1, filters)
  }

  const handleFilterChange = (key, val) => {
    const newFilters = { ...filters, [key]: val }
    setFilters(newFilters)
    if (username) fetchGames(username, 1, newFilters)
  }

  const totalPages = Math.ceil(total / PAGE_SIZE)

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="logo">
          <span className="logo-icon">♞</span>
          <div>
            <div className="logo-title">LICHESS</div>
            <div className="logo-sub">GAMES EXPLORER</div>
          </div>
        </div>

        <div className="sidebar-section">
          <label className="section-label">USERNAME</label>
          <div className="input-row">
            <input
              className="text-input"
              placeholder="e.g. DrNykterstein"
              value={inputUser}
              onChange={e => setInputUser(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleLoad()}
            />
            <button className="btn-primary" onClick={handleLoad}>LOAD</button>
          </div>
        </div>

        {users.length > 0 && (
          <div className="sidebar-section">
            <label className="section-label">CACHED USERS</label>
            <div className="user-list">
              {users.map(u => (
                <button
                  key={u.username}
                  className={`user-chip ${username === u.username ? 'active' : ''}`}
                  onClick={() => {
                    setInputUser(u.username)
                    setUsername(u.username)
                    fetchGames(u.username, 1, filters)
                  }}
                >
                  <span className="user-chip-name">{u.username}</span>
                  <span className="user-chip-count">{u.game_count}g</span>
                </button>
              ))}
            </div>
          </div>
        )}

        <div className="sidebar-section">
          <label className="section-label">FILTERS</label>
          <div className="filter-group">
            <select
              className="select-input"
              value={filters.color}
              onChange={e => handleFilterChange('color', e.target.value)}
            >
              <option value="">All Colors</option>
              <option value="white">White</option>
              <option value="black">Black</option>
            </select>
            <select
              className="select-input"
              value={filters.result}
              onChange={e => handleFilterChange('result', e.target.value)}
            >
              <option value="">All Results</option>
              <option value="1-0">White Won</option>
              <option value="0-1">Black Won</option>
              <option value="1/2-1/2">Draw</option>
            </select>
            <select
              className="select-input"
              value={filters.perf_type}
              onChange={e => handleFilterChange('perf_type', e.target.value)}
            >
              <option value="">All Types</option>
              <option value="bullet">Bullet</option>
              <option value="blitz">Blitz</option>
              <option value="rapid">Rapid</option>
              <option value="classical">Classical</option>
            </select>
          </div>
        </div>

        <div className="sidebar-spacer" />

        <button
          className="btn-refresh"
          onClick={() => setShowRefresh(true)}
        >
          <span>⟳</span> REFRESH USER DATA
        </button>

        {lastUpdated && (
          <div className="last-updated">
            Last synced:<br />
            {new Date(lastUpdated).toLocaleString()}
          </div>
        )}
      </aside>

      <main className="main">
        {!username ? (
          <div className="empty-state">
            <div className="empty-board">
              {['♜','♞','♝','♛','♚','♝','♞','♜'].map((p, i) => (
                <span key={i} className="empty-piece" style={{ animationDelay: `${i * 0.1}s` }}>{p}</span>
              ))}
            </div>
            <h1 className="empty-title">Enter a username to begin</h1>
            <p className="empty-sub">Load a Lichess user then refresh to fetch their games from the API.</p>
          </div>
        ) : (
          <>
            <div className="main-header">
              <div>
                <h2 className="main-title">{username}</h2>
                <div className="main-meta">
                  {total > 0 ? `${total} games` : 'No games found'}
                  {loading && <span className="loading-dot"> ···</span>}
                </div>
              </div>
              {totalPages > 1 && (
                <div className="pagination">
                  <button
                    className="page-btn"
                    disabled={page <= 1}
                    onClick={() => fetchGames(username, page - 1)}
                  >←</button>
                  <span className="page-info">{page} / {totalPages}</span>
                  <button
                    className="page-btn"
                    disabled={page >= totalPages}
                    onClick={() => fetchGames(username, page + 1)}
                  >→</button>
                </div>
              )}
            </div>

            {error && <div className="error-banner">{error}</div>}

            {games.length === 0 && !loading && !error && (
              <div className="no-games">
                <p>No games found. Try refreshing data from Lichess.</p>
                <button className="btn-primary" onClick={() => setShowRefresh(true)}>
                  ⟳ REFRESH USER DATA
                </button>
              </div>
            )}

            <GameList
              games={games}
              username={username}
              loading={loading}
              onSelect={setSelectedGame}
            />
          </>
        )}
      </main>

      {showRefresh && (
        <RefreshModal
          defaultUsername={username || inputUser}
          onClose={() => setShowRefresh(false)}
          onDone={handleRefreshDone}
        />
      )}

      {selectedGame && (
        <GameDetail
          game={selectedGame}
          username={username}
          onClose={() => setSelectedGame(null)}
        />
      )}
    </div>
  )
}
