import { useState, useEffect, useCallback, useRef } from 'react'
import './ReviewQueue.css'

function PhaseBar({ label, blunders, pct }) {
  return (
    <div className="phase-row">
      <span className="phase-label">{label}</span>
      <div className="phase-bar-track">
        <div className="phase-bar-fill" style={{ width: `${pct}%` }} />
      </div>
      <span className="phase-count">{blunders} <span className="phase-pct">({pct}%)</span></span>
    </div>
  )
}

function GameRow({ game, username, onSelectGame }) {
  const handleClick = async () => {
    try {
      const r = await fetch(`/api/game/${username}/${game.game_id}`)
      const fullGame = await r.json()
      onSelectGame(fullGame)
    } catch {
      onSelectGame(game)
    }
  }

  const isWhite = game.white?.toLowerCase() === username?.toLowerCase()
  const opponent = isWhite ? game.black : game.white
  const oppElo = isWhite ? game.black_elo : game.white_elo

  let resultLabel = '½'
  let resultClass = 'result-draw'
  if (game.result === '1-0') {
    resultLabel = isWhite ? 'W' : 'L'
    resultClass = isWhite ? 'result-win' : 'result-loss'
  } else if (game.result === '0-1') {
    resultLabel = isWhite ? 'L' : 'W'
    resultClass = isWhite ? 'result-loss' : 'result-win'
  }

  const dateStr = game.date ? game.date.slice(0, 7) : ''

  return (
    <div className="review-game-row" onClick={handleClick}>
      <span className={`result-badge ${resultClass}`}>{resultLabel}</span>
      <div className="review-game-info">
        <div className="review-game-top">
          <span className="opponent-name">{opponent}</span>
          {oppElo && <span className="opponent-elo">({oppElo})</span>}
          {game.opening && <span className="game-opening">{game.opening}</span>}
        </div>
        <div className="review-game-bottom">
          <span className="game-date">{dateStr}</span>
          {game.perf_type && <span className="game-perf">{game.perf_type}</span>}
          {game.critical_move_number && (
            <span className="critical-move">move {game.critical_move_number}</span>
          )}
        </div>
      </div>
      <div className="mistake-badges">
        {game.blunder_count > 0 && (
          <span className="badge badge-blunder">🔴 ×{game.blunder_count}</span>
        )}
        {game.mistake_count > 0 && (
          <span className="badge badge-mistake">🟠 ×{game.mistake_count}</span>
        )}
        {game.inaccuracy_count > 0 && (
          <span className="badge badge-inaccuracy">🟡 ×{game.inaccuracy_count}</span>
        )}
        {game.biggest_drop_cp > 0 && (
          <span className={`drop-indicator ${game.biggest_drop_cp > 300 ? 'drop-blunder' : game.biggest_drop_cp > 150 ? 'drop-mistake' : 'drop-inaccuracy'}`}>
            ⬇ {game.biggest_drop_cp}cp
          </span>
        )}
      </div>
    </div>
  )
}

export default function ReviewQueue({ username, appliedFilters = {}, onSelectGame }) {
  const [patterns, setPatterns] = useState(null)
  const [patternsLoading, setPatternsLoading] = useState(true)
  const [queue, setQueue] = useState(null)
  const [queueLoading, setQueueLoading] = useState(true)
  const [page, setPage] = useState(1)
  const [sortBy, setSortBy] = useState('blunder_count')
  const [blundersOnly, setBlundersOnly] = useState(false)
  const appliedFiltersRef = useRef(appliedFilters)

  const PAGE_SIZE = 50

  // Keep ref in sync so callbacks always read latest filters
  useEffect(() => { appliedFiltersRef.current = appliedFilters }, [appliedFilters])

  const fetchPatterns = useCallback(async () => {
    if (!username) return
    const af = appliedFiltersRef.current
    setPatternsLoading(true)
    try {
      const params = new URLSearchParams()
      if (af.color) params.set('color', af.color)
      if (af.outcome) params.set('outcome', af.outcome)
      if (af.perf_type) params.set('perf_type', af.perf_type)
      if (af.since_date) params.set('since_date', af.since_date)
      if (af.until_date) params.set('until_date', af.until_date)
      if (af.opening) params.set('opening', af.opening)
      if (af.termination) params.set('termination', af.termination)
      const r = await fetch(`/api/mistake_patterns/${username}?${params}`)
      setPatterns(await r.json())
    } catch {
      setPatterns(null)
    } finally {
      setPatternsLoading(false)
    }
  }, [username])

  const fetchQueue = useCallback(async (p = 1, sort = sortBy) => {
    if (!username) return
    const af = appliedFiltersRef.current
    setQueueLoading(true)
    try {
      const params = new URLSearchParams({ page: p, page_size: PAGE_SIZE, sort_by: sort })
      if (af.color) params.set('color', af.color)
      if (af.outcome) params.set('outcome', af.outcome)
      if (af.perf_type) params.set('perf_type', af.perf_type)
      if (af.since_date) params.set('since_date', af.since_date)
      if (af.until_date) params.set('until_date', af.until_date)
      if (af.opening) params.set('opening', af.opening)
      if (af.termination) params.set('termination', af.termination)
      if (af.min_moves) params.set('min_moves', af.min_moves)
      if (af.max_moves) params.set('max_moves', af.max_moves)
      if (af.bookmarked_only) params.set('bookmarked_only', 'true')
      const r = await fetch(`/api/review/${username}?${params}`)
      setQueue(await r.json())
      setPage(p)
    } catch {
      setQueue(null)
    } finally {
      setQueueLoading(false)
    }
  }, [username, sortBy])

  useEffect(() => {
    fetchPatterns()
    fetchQueue(1, sortBy)
  }, [username, appliedFilters, fetchPatterns, fetchQueue]) // eslint-disable-line react-hooks/exhaustive-deps

  const handleSortChange = (newSort) => {
    setSortBy(newSort)
    fetchQueue(1, newSort)
  }

  const noEvals = patterns?.no_evals || queue?.no_evals

  if (noEvals) {
    return (
      <div className="review-empty">
        <div className="review-empty-icon">⚡</div>
        <h2>No evaluations yet</h2>
        <p>Run Stockfish evaluations on your games first to unlock the Review Queue.</p>
      </div>
    )
  }

  const games = queue?.games ?? []
  const filteredGames = blundersOnly ? games.filter(g => g.blunder_count > 0) : games
  const totalPages = Math.ceil((queue?.total ?? 0) / PAGE_SIZE)

  return (
    <div className="review-root">
      {/* ── Patterns Section ── */}
      <section className="patterns-section">
        <h3 className="section-title">Mistake Patterns</h3>
        {patternsLoading ? (
          <div className="review-loading">Loading patterns···</div>
        ) : patterns && !patterns.no_evals ? (
          <div className="patterns-grid">
            {/* Summary card */}
            <div className="pattern-card summary-card">
              <div className="pattern-card-title">Summary</div>
              <div className="summary-stat">
                <span className="summary-num">{patterns.summary?.evaluated_games ?? 0}</span>
                <span className="summary-lbl">evaluated games</span>
              </div>
              <div className="summary-stat">
                <span className="summary-num blunder-color">{patterns.summary?.total_blunders ?? 0}</span>
                <span className="summary-lbl">total blunders</span>
              </div>
              <div className="summary-stat">
                <span className="summary-num">{patterns.summary?.avg_per_game ?? 0}</span>
                <span className="summary-lbl">avg blunders / game</span>
              </div>
            </div>

            {/* Phase distribution */}
            <div className="pattern-card phase-card">
              <div className="pattern-card-title">Blunders by Phase</div>
              {patterns.phase_distribution?.length > 0 ? (
                patterns.phase_distribution.map(p => (
                  <PhaseBar key={p.phase} label={p.phase} blunders={p.blunders} pct={p.pct} />
                ))
              ) : (
                <div className="no-data-msg">No blunder data</div>
              )}
            </div>

            {/* Worst openings */}
            <div className="pattern-card openings-card">
              <div className="pattern-card-title">Worst Openings (blunders/game)</div>
              {patterns.worst_openings?.length > 0 ? (
                <div className="openings-list">
                  {patterns.worst_openings.map((o, i) => (
                    <div key={i} className="opening-row">
                      <span className="opening-name" title={o.opening}>{o.opening}</span>
                      <span className="opening-rate blunder-color">{o.blunder_rate}</span>
                      <span className="opening-games">{o.games}g</span>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="no-data-msg">Not enough data (min 3 games per opening)</div>
              )}
            </div>
          </div>
        ) : (
          <div className="no-data-msg">No pattern data available.</div>
        )}
      </section>

      {/* ── Review Queue ── */}
      <section className="queue-section">
        <div className="queue-header">
          <h3 className="section-title">
            Review Queue
            {queue?.total != null && <span className="queue-count"> ({queue.total} games)</span>}
          </h3>
          <div className="queue-controls">
            <label className="control-label">Sort:</label>
            <select
              className="review-select"
              value={sortBy}
              onChange={e => handleSortChange(e.target.value)}
            >
              <option value="blunder_count">Most Blunders</option>
              <option value="biggest_drop">Biggest Drop</option>
            </select>
            <label className="checkbox-label">
              <input
                type="checkbox"
                checked={blundersOnly}
                onChange={e => setBlundersOnly(e.target.checked)}
              />
              Blunders only
            </label>
          </div>
        </div>

        {queueLoading ? (
          <div className="review-loading">Loading queue···</div>
        ) : filteredGames.length === 0 ? (
          <div className="no-data-msg">No games match the current filters.</div>
        ) : (
          <div className="queue-list">
            {filteredGames.map(game => (
              <GameRow
                key={game.game_id}
                game={game}
                username={username}
                onSelectGame={onSelectGame}
              />
            ))}
          </div>
        )}

        {totalPages > 1 && !blundersOnly && (
          <div className="queue-pagination">
            <button
              className="page-btn"
              disabled={page <= 1}
              onClick={() => fetchQueue(page - 1)}
            >←</button>
            <span className="page-info">{page} / {totalPages}</span>
            <button
              className="page-btn"
              disabled={page >= totalPages}
              onClick={() => fetchQueue(page + 1)}
            >→</button>
          </div>
        )}
      </section>
    </div>
  )
}
