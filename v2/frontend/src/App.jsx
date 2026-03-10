import { useState, useEffect, useCallback, useRef } from 'react'
import GameList from './components/GameList.jsx'
import RefreshModal from './components/RefreshModal.jsx'
import GameDetail from './components/GameDetail.jsx'
import SettingsModal from './components/SettingsModal.jsx'
import EvalLogModal from './components/EvalLogModal.jsx'
import Dashboard from './components/Dashboard.jsx'
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
  const [activeTab, setActiveTab] = useState('explorer')
  const [loading, setLoading] = useState(false)
  const [connecting, setConnecting] = useState(true)
  const [evalRunning, setEvalRunning] = useState(false)
  const [showEvalLog, setShowEvalLog] = useState(false)
  const [error, setError] = useState(null)
  const [noData, setNoData] = useState(false)
  const [showRefresh, setShowRefresh] = useState(false)
  const [showSettings, setShowSettings] = useState(false)
  const evalPollRef = useRef(null)
  const [selectedGame, setSelectedGame] = useState(null)
  const [selectedGameEvals, setSelectedGameEvals] = useState(null)
  const [filters, setFilters] = useState({ color: '', outcome: '', perf_type: '', since_date: '', until_date: '', bookmarked_only: false, opening: '', evaluated_only: false })
  const [openingsList, setOpeningsList] = useState([])

  const PAGE_SIZE = 100

  const fetchStatus = useCallback(async (user) => {
    try {
      const r = await fetch(`/api/status/${user}`)
      const d = await r.json()
      setLastDate(d.last_date)
    } catch {}
  }, [])

  const fetchEvalStatus = useCallback(async (user) => {
    try {
      const r = await fetch(`/api/evaluate/status/${user}`)
      const d = await r.json()
      setEvalRunning(d.running)
    } catch {}
  }, [])

  const fetchEvalCount = useCallback(async (user, f) => {
    try {
      const params = new URLSearchParams({
        ...(f.color         && { color: f.color }),
        ...(f.outcome       && { outcome: f.outcome }),
        ...(f.perf_type     && { perf_type: f.perf_type }),
        ...(f.since_date    && { since_date: f.since_date }),
        ...(f.until_date    && { until_date: f.until_date }),
        ...(f.bookmarked_only && { bookmarked_only: 'true' }),
        ...(f.opening       && { opening: f.opening }),
      })
      const r = await fetch(`/api/evaluate/count/${user}?${params}`)
      const d = await r.json()
      setUnevaluatedCount(d.count ?? 0)
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
        ...(f.opening && { opening: f.opening }),
        ...(f.evaluated_only && { evaluated_only: 'true' }),
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

  const fetchOpenings = useCallback(async (user) => {
    try {
      const r = await fetch(`/api/openings/${user}`)
      setOpeningsList(await r.json())
    } catch {}
  }, [])

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

  // Load settings on mount, apply any URL params, then auto-load games
  // Retries on failure so the page recovers automatically after a backend restart
  useEffect(() => {
    const urlParams = new URLSearchParams(window.location.search)
    const urlOpening    = urlParams.get('opening')    || ''
    const urlSinceDate  = urlParams.get('since_date') || ''
    const urlUntilDate  = urlParams.get('until_date') || ''
    const urlTab        = urlParams.get('tab')         || 'explorer'
    const initialFilters = { color: '', outcome: '', perf_type: '', since_date: urlSinceDate, until_date: urlUntilDate, bookmarked_only: false, opening: urlOpening, evaluated_only: false }
    if (urlTab !== 'explorer') setActiveTab(urlTab)
    setFilters(initialFilters)

    let cancelled = false
    const MAX_ATTEMPTS = 30
    const MAX_DELAY_MS = 4000

    async function loadSettings() {
      for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
        if (cancelled) return
        try {
          const r = await fetch('/api/settings')
          if (!r.ok) throw new Error('not ready')
          const d = await r.json()
          if (cancelled) return
          const user = d.default_user || 'luckleland'
          setDefaultUser(user)
          setHasToken(d.has_token || false)
          setConnecting(false)
          fetchGames(user, 1, initialFilters)
          fetchStatus(user)
          fetchBookmarks(user)
          fetchEvalStatus(user)
          fetchEvalCount(user, initialFilters)
          fetchOpenings(user)
          return
        } catch {
          if (cancelled) return
          const delay = Math.min(500 * (attempt + 1), MAX_DELAY_MS)
          await new Promise(res => setTimeout(res, delay))
        }
      }
      if (!cancelled) {
        setConnecting(false)
        setError('Could not connect to backend after multiple attempts.')
      }
    }

    loadSettings()
    return () => { cancelled = true }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // Poll eval status every 5s when running, every 30s otherwise
  useEffect(() => {
    if (!defaultUser) return
    const interval = evalRunning ? 5000 : 30000
    clearInterval(evalPollRef.current)
    evalPollRef.current = setInterval(() => fetchEvalStatus(defaultUser), interval)
    return () => clearInterval(evalPollRef.current)
  }, [defaultUser, evalRunning, fetchEvalStatus])

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
    const resetFilters = { color: '', outcome: '', perf_type: '', since_date: '', until_date: '', bookmarked_only: false, opening: '', evaluated_only: false }
    setFilters(resetFilters)
    fetchGames(newUser, 1, resetFilters)
    fetchStatus(newUser)
    fetchEvalCount(newUser, resetFilters)
    fetchBookmarks(newUser)
  }

  const handleFilterChange = (key, val) => {
    const newFilters = { ...filters, [key]: val }
    setFilters(newFilters)
    fetchGames(defaultUser, 1, newFilters)
    fetchEvalCount(defaultUser, newFilters)
  }

  const handleSliderDateChange = (since, until) => {
    const newFilters = { ...filters, since_date: since, until_date: until, opening: '' }
    setFilters(newFilters)
    fetchEvalCount(defaultUser, newFilters)
  }

  const handleOpeningSelect = (openingName, sinceYM, untilYM) => {
    const sinceDate = sinceYM ? sinceYM + '-01' : ''
    const untilDate = untilYM ? (() => { const [y, m] = untilYM.split('-').map(Number); return `${untilYM}-${new Date(y, m, 0).getDate()}` })() : ''
    const newFilters = { ...filters, opening: openingName, since_date: sinceDate, until_date: untilDate }
    setFilters(newFilters)
    setActiveTab('explorer')
    fetchGames(defaultUser, 1, newFilters)
    fetchEvalCount(defaultUser, newFilters)
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
    try {
      const r = await fetch('/api/evaluate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          username: defaultUser,
          depth: 15,
          color:          filters.color          || null,
          outcome:        filters.outcome        || null,
          perf_type:      filters.perf_type      || null,
          since_date:     filters.since_date     || null,
          until_date:     filters.until_date     || null,
          bookmarked_only: filters.bookmarked_only || false,
          opening:        filters.opening        || null,
        }),
      })
      const d = await r.json()
      if (d.started) setEvalRunning(true)
    } catch (e) {
      setError(e.message)
    }
    setShowEvalLog(true)
  }

  const handleEvalStatusUpdate = (status) => {
    setEvalRunning(status.running)
    if (!status.running) {
      fetchEvalCount(defaultUser, filters)
    }
  }

  const totalPages = Math.ceil(total / PAGE_SIZE)

  if (connecting) {
    return (
      <div className="app">
        <div className="empty-state">
          <div className="empty-board">
            {['♜','♞','♝','♛','♚','♝','♞','♜'].map((p, i) => (
              <span key={i} className="empty-piece" style={{ animationDelay: `${i * 0.1}s` }}>{p}</span>
            ))}
          </div>
          <h1 className="empty-title">Connecting to backend</h1>
          <p className="empty-sub">Waiting for the server to become ready<span className="loading-dot"> ···</span></p>
        </div>
      </div>
    )
  }

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
                  const cleared = { color: '', outcome: '', perf_type: '', since_date: '', until_date: '', bookmarked_only: false, opening: '', evaluated_only: false }
                  setFilters(cleared)
                  fetchGames(defaultUser, 1, cleared)
                  fetchEvalCount(defaultUser, cleared)
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
            <div className="date-filter-wrap">
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
              <div className="date-quick-btns">
                {[
                  { label: '30D', days: 30 },
                  { label: '90D', days: 90 },
                  { label: '6M',  days: 180 },
                  { label: '1Y',  days: 365 },
                ].map(({ label, days }) => {
                  const since = new Date(Date.now() - days * 86400000).toISOString().slice(0, 10)
                  const active = filters.since_date === since && !filters.until_date
                  return (
                    <button
                      key={label}
                      className={`date-quick-btn ${active ? 'active' : ''}`}
                      onClick={() => {
                        const newF = { ...filters, since_date: since, until_date: '' }
                        setFilters(newF)
                        fetchGames(defaultUser, 1, newF)
                        fetchEvalCount(defaultUser, newF)
                      }}
                    >{label}</button>
                  )
                })}
                <button
                  className={`date-quick-btn ${!filters.since_date && !filters.until_date ? 'active' : ''}`}
                  onClick={() => {
                    const newF = { ...filters, since_date: '', until_date: '' }
                    setFilters(newF)
                    fetchGames(defaultUser, 1, newF)
                    fetchEvalCount(defaultUser, newF)
                  }}
                >All</button>
              </div>
            </div>
            <select
              className="select-input"
              value={filters.opening}
              onChange={e => handleFilterChange('opening', e.target.value)}
            >
              <option value="">All Openings</option>
              {openingsList.map(op => (
                <option key={op} value={op}>{op}</option>
              ))}
            </select>
            <button
              className={`btn-bookmark-filter ${filters.bookmarked_only ? 'active' : ''}`}
              onClick={() => handleFilterChange('bookmarked_only', !filters.bookmarked_only)}
            >
              ★ Bookmarked only
              {bookmarks.size > 0 && <span className="bookmark-count">{bookmarks.size}</span>}
            </button>
            <button
              className={`btn-bookmark-filter ${filters.evaluated_only ? 'active' : ''}`}
              onClick={() => handleFilterChange('evaluated_only', !filters.evaluated_only)}
            >
              ⚡ Evaluated only
            </button>
          </div>
        </div>

        <div className="sidebar-spacer" />

        {filters.since_date && (
          <button
            className="btn-show-games"
            onClick={() => {
              fetchGames(defaultUser, 1, filters)
              setActiveTab('explorer')
            }}
          >
            ♟ SHOW GAMES
          </button>
        )}

        {evalRunning ? (
          <button
            className="btn-eval btn-eval-running"
            onClick={() => setShowEvalLog(true)}
          >
            ⚡ EVALUATIONS RUNNING
          </button>
        ) : unevaluatedCount > 0 ? (
          <button
            className="btn-eval"
            onClick={handleRunEvals}
          >
            ⚡ RUN EVALUATIONS ({unevaluatedCount})
          </button>
        ) : null}

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
        <div className="tab-bar">
          <button
            className={`tab-btn ${activeTab === 'explorer' ? 'tab-btn--active' : ''}`}
            onClick={() => setActiveTab('explorer')}
          >
            ♟ GAMES EXPLORER
          </button>
          <button
            className={`tab-btn ${activeTab === 'dashboard' ? 'tab-btn--active' : ''}`}
            onClick={() => setActiveTab('dashboard')}
          >
            ◈ ANALYSIS
          </button>
        </div>

        {activeTab === 'dashboard' ? (
          <div className="dashboard-scroll">
            <Dashboard username={defaultUser} onOpeningSelect={handleOpeningSelect} onSliderChange={handleSliderDateChange} />
          </div>
        ) : noData ? (
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

      {showEvalLog && (
        <EvalLogModal
          username={defaultUser}
          onClose={() => setShowEvalLog(false)}
          onStatusUpdate={handleEvalStatusUpdate}
        />
      )}
    </div>
  )
}
