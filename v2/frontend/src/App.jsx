import { useState, useEffect, useCallback } from 'react'
import GameList from './components/GameList.jsx'
import RefreshModal from './components/RefreshModal.jsx'
import GameDetail from './components/GameDetail.jsx'
import SettingsModal from './components/SettingsModal.jsx'
import './App.css'

export default function App() {
  const [defaultUser, setDefaultUser] = useState('')
  const [hasToken, setHasToken] = useState(false)
  const [bookmarks, setBookmarks] = useState(new Set())
  const [bookmarkSyncing, setBookmarkSyncing] = useState(false)
  const [games, setGames] = useState([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [lastDate, setLastDate] = useState(null)
  const [unevaluatedCount, setUnevaluatedCount] = useState(0)
  const [loading, setLoading] = useState(false)
  const [evalLoading, setEvalLoading] = useState(false)
  const [error, setError] = useState(null)
  const [noData, setNoData] = useState(false)
  const [showRefresh, setShowRefresh] = useState(false)
  const [showSettings, setShowSettings] = useState(false)
  const [selectedGame, setSelectedGame] = useState(null)
  const [selectedGameEvals, setSelectedGameEvals] = useState(null)
  const [filters, setFilters] = useState({ color: '', outcome: '', perf_type: '', since_date: '', until_date: '', bookmarked_only: false })

  const PAGE_SIZE = 100

  const fetchStatus = useCallback(async (user) => {
    try {
      const r = await fetch(`/api/status/${user}`)
      const d = await r.json()
      setLastDate(d.last_date)
      setUnevaluatedCount(d.unevaluated_count)
    } catch {}
  }, [])

  const fetchGames = useCallback(async (user, p = 1, f = filters) => {
    if (!user) return
    setLoading(true)
    setError(null)
    setNoData(false)
    try {
      const params = new URLSearchParams({
        page: p,
        page_size: PAGE_SIZE,
        ...(f.color && { color: f.color }),
        ...(f.outcome && { outcome: f.outcome }),
        ...(f.perf_type && { perf_type: f.perf_type }),
        ...(f.since_date && { since_date: f.since_date }),
        ...(f.until_date && { until_date: f.until_date }),
        ...(f.bookmarked_only && { bookmarked_only: 'true' }),
      })
      const r = await fetch(`/api/games/${user}?${params}`)
      if (!r.ok) {
        const err = await r.json()
        throw new Error(err.detail || 'Failed to load games')
      }
      const d = await r.json()
      if (d.no_data) {
        setNoData(true)
        setGames([])
        setTotal(0)
      } else {
        setGames(d.games)
        setTotal(d.total)
        setPage(p)
      }
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [filters])

  const fetchBookmarks = useCallback(async (user) => {
    try {
      const r = await fetch(`/api/bookmarks/${user}`)
      const ids = await r.json()
      setBookmarks(new Set(ids))
    } catch {}
  }, [])

  const handleSyncBookmarks = async () => {
    setBookmarkSyncing(true)
    try {
      const r = await fetch('/api/bookmarks/sync', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: defaultUser }),
      })
      const d = await r.json()
      if (!r.ok) throw new Error(d.detail || 'Sync failed')
      await fetchBookmarks(defaultUser)
    } catch (e) {
      setError(e.message)
    } finally {
      setBookmarkSyncing(false)
    }
  }

  // Load settings on mount, then auto-load games
  useEffect(() => {
    fetch('/api/settings')
      .then(r => r.json())
      .then(d => {
        const user = d.default_user || 'luckleland'
        setDefaultUser(user)
        setHasToken(d.has_token || false)
        fetchGames(user, 1, filters)
        fetchStatus(user)
        fetchBookmarks(user)
      })
      .catch(() => {})
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const handleRefreshDone = (result) => {
    setShowRefresh(false)
    fetchGames(defaultUser, 1, filters)
    fetchStatus(defaultUser)
  }

  const handleSettingsSave = (newUser, newHasToken) => {
    setShowSettings(false)
    setHasToken(newHasToken || false)
    setDefaultUser(newUser)
    setPage(1)
    setGames([])
    setNoData(false)
    fetchGames(newUser, 1, filters)
    fetchStatus(newUser)
    fetchBookmarks(newUser)
  }

  const handleFilterChange = (key, val) => {
    const newFilters = { ...filters, [key]: val }
    setFilters(newFilters)
    fetchGames(defaultUser, 1, newFilters)
  }

  const handleSelectGame = async (game) => {
    setSelectedGame(game)
    setSelectedGameEvals(null)
    try {
      const r = await fetch(`/api/evals/${defaultUser}/${game.game_id}`)
      const evals = await r.json()
      setSelectedGameEvals(evals)
    } catch {}
  }

  const handleRunEvals = async () => {
    setEvalLoading(true)
    try {
      const r = await fetch('/api/evaluate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: defaultUser, batch_size: 50, depth: 15 }),
      })
      const d = await r.json()
      setUnevaluatedCount(d.remaining || 0)
    } catch (e) {
      setError(e.message)
    } finally {
      setEvalLoading(false)
    }
  }

  const totalPages = Math.ceil(total / PAGE_SIZE)

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="logo">
          <span className="logo-icon">♞</span>
          <div className="logo-text">
            <div className="logo-title">LICHESS</div>
            <div className="logo-sub">GAMES EXPLORER</div>
          </div>
          <button className="btn-gear" onClick={() => setShowSettings(true)} title="Settings">
            ⚙
          </button>
        </div>

        <div className="sidebar-section">
          <div className="filter-header">
            <label className="section-label">FILTERS</label>
            {Object.values(filters).some(Boolean) && (
              <button
                className="btn-clear-filters"
                onClick={() => {
                  const cleared = { color: '', outcome: '', perf_type: '', since_date: '', until_date: '', bookmarked_only: false }
                  setFilters(cleared)
                  fetchGames(defaultUser, 1, cleared)
                }}
              >
                CLEAR
              </button>
            )}
          </div>
          <div className="filter-group">
            <select
              className="select-input"
              value={filters.outcome}
              onChange={e => handleFilterChange('outcome', e.target.value)}
            >
              <option value="">All Results</option>
              <option value="win">{defaultUser || 'User'} Won</option>
              <option value="loss">{defaultUser || 'User'} Lost</option>
              <option value="draw">Draw</option>
            </select>
            <select
              className="select-input"
              value={filters.color}
              onChange={e => handleFilterChange('color', e.target.value)}
            >
              <option value="">All Colors</option>
              <option value="white">Played White</option>
              <option value="black">Played Black</option>
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
            <div className="date-filter-row">
              <input
                type="date"
                className="date-input"
                title="From date"
                value={filters.since_date}
                onChange={e => handleFilterChange('since_date', e.target.value)}
              />
              <span className="date-sep">–</span>
              <input
                type="date"
                className="date-input"
                title="To date"
                value={filters.until_date}
                onChange={e => handleFilterChange('until_date', e.target.value)}
              />
            </div>
            <button
              className={`btn-bookmark-filter ${filters.bookmarked_only ? 'active' : ''}`}
              onClick={() => handleFilterChange('bookmarked_only', !filters.bookmarked_only)}
            >
              ★ Bookmarked only
              {bookmarks.size > 0 && <span className="bookmark-count">{bookmarks.size}</span>}
            </button>
          </div>
        </div>

        <div className="sidebar-spacer" />

        {unevaluatedCount > 0 && (
          <button
            className="btn-eval"
            onClick={handleRunEvals}
            disabled={evalLoading}
          >
            {evalLoading ? '⟳ EVALUATING···' : `⚡ RUN EVALUATIONS (${unevaluatedCount})`}
          </button>
        )}

        {hasToken && (
          <button
            className="btn-refresh"
            onClick={handleSyncBookmarks}
            disabled={bookmarkSyncing}
          >
            <span>★</span> {bookmarkSyncing ? 'SYNCING···' : 'SYNC BOOKMARKS'}
          </button>
        )}

        <button
          className="btn-refresh"
          onClick={() => setShowRefresh(true)}
        >
          <span>⟳</span> REFRESH DATA
        </button>

        {lastDate && (
          <div className="last-updated">
            Last data: {lastDate}
          </div>
        )}
      </aside>

      <main className="main">
        {noData ? (
          <div className="empty-state">
            <div className="empty-board">
              {['♜','♞','♝','♛','♚','♝','♞','♜'].map((p, i) => (
                <span key={i} className="empty-piece" style={{ animationDelay: `${i * 0.1}s` }}>{p}</span>
              ))}
            </div>
            <h1 className="empty-title">No data for {defaultUser}</h1>
            <p className="empty-sub">Run a full refresh to fetch all games from Lichess.</p>
            <button className="btn-primary" onClick={() => setShowRefresh(true)}>
              ↺ FULL REFRESH
            </button>
          </div>
        ) : defaultUser ? (
          <>
            <div className="main-header">
              <div>
                <h2 className="main-title">{defaultUser}</h2>
                <div className="main-meta">
                  {total > 0 ? `${total} games` : loading ? '' : 'No games found'}
                  {loading && <span className="loading-dot"> ···</span>}
                </div>
              </div>
              {totalPages > 1 && (
                <div className="pagination">
                  <button
                    className="page-btn"
                    disabled={page <= 1}
                    onClick={() => fetchGames(defaultUser, page - 1)}
                  >←</button>
                  <span className="page-info">{page} / {totalPages}</span>
                  <button
                    className="page-btn"
                    disabled={page >= totalPages}
                    onClick={() => fetchGames(defaultUser, page + 1)}
                  >→</button>
                </div>
              )}
            </div>

            {error && <div className="error-banner">{error}</div>}

            <GameList
              games={games}
              username={defaultUser}
              loading={loading}
              onSelect={handleSelectGame}
              bookmarks={bookmarks}
            />
          </>
        ) : (
          <div className="empty-state">
            <div className="loading-dot" style={{ fontSize: 24 }}>···</div>
          </div>
        )}
      </main>

      {showRefresh && (
        <RefreshModal
          username={defaultUser}
          lastDate={lastDate}
          onClose={() => setShowRefresh(false)}
          onDone={handleRefreshDone}
        />
      )}

      {showSettings && (
        <SettingsModal
          currentUser={defaultUser}
          hasToken={hasToken}
          onClose={() => setShowSettings(false)}
          onSave={handleSettingsSave}
        />
      )}

      {selectedGame && (
        <GameDetail
          game={selectedGame}
          username={defaultUser}
          evals={selectedGameEvals}
          onClose={() => { setSelectedGame(null); setSelectedGameEvals(null) }}
        />
      )}
    </div>
  )
}
