import { useState, useEffect, useCallback, useRef } from 'react'
import ReviewLogModal from './ReviewLogModal.jsx'
import './ReviewQueue.css'

// ── Mistake Patterns section (unchanged) ──────────────────────────────────────

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

// ── Sortable reviews grid ─────────────────────────────────────────────────────

const COLUMNS = [
  { key: 'date',            label: 'Date',       sortable: true  },
  { key: 'opponent',        label: 'Opponent',   sortable: false },
  { key: 'result',          label: 'Result',     sortable: false },
  { key: 'opening',         label: 'Opening',    sortable: false },
  { key: 'blunder_count',   label: 'Blunders',   sortable: true  },
  { key: 'mistake_count',   label: 'Mistakes',   sortable: true  },
  { key: 'inaccuracy_count',label: 'Inaccuracies',sortable: true },
  { key: 'biggest_drop_cp', label: 'Biggest Drop',sortable: true },
  { key: 'critical_phase',  label: 'Phase',      sortable: false },
]

function SortIcon({ active, dir }) {
  if (!active) return <span className="sort-icon sort-icon--inactive">↕</span>
  return <span className="sort-icon sort-icon--active">{dir === 'asc' ? '↑' : '↓'}</span>
}

function ReviewRow({ review, username, onSelect }) {
  const isWhite = (review.white || '').toLowerCase() === username?.toLowerCase()
  const opponent = isWhite ? review.black : review.white
  const oppElo   = isWhite ? review.black_elo : review.white_elo

  let resultLabel = '½'
  let resultClass = 'result-draw'
  if (review.result === '1-0') {
    resultLabel = isWhite ? 'W' : 'L'
    resultClass = isWhite ? 'result-win' : 'result-loss'
  } else if (review.result === '0-1') {
    resultLabel = isWhite ? 'L' : 'W'
    resultClass = isWhite ? 'result-loss' : 'result-win'
  }

  return (
    <tr className="review-grid-row" onClick={() => onSelect(review)}>
      <td className="col-date">{review.date ? review.date.slice(0, 7) : '—'}</td>
      <td className="col-opponent">
        <span className="opponent-name">{opponent}</span>
        {oppElo && <span className="opponent-elo"> ({oppElo})</span>}
      </td>
      <td className="col-result">
        <span className={`result-badge ${resultClass}`}>{resultLabel}</span>
      </td>
      <td className="col-opening" title={review.opening || ''}>
        {review.opening || '—'}
      </td>
      <td className="col-num col-blunder">
        {review.blunder_count > 0
          ? <span className="badge badge-blunder">🔴 {review.blunder_count}</span>
          : <span className="col-zero">0</span>}
      </td>
      <td className="col-num col-mistake">
        {review.mistake_count > 0
          ? <span className="badge badge-mistake">🟠 {review.mistake_count}</span>
          : <span className="col-zero">0</span>}
      </td>
      <td className="col-num col-inaccuracy">
        {review.inaccuracy_count > 0
          ? <span className="badge badge-inaccuracy">🟡 {review.inaccuracy_count}</span>
          : <span className="col-zero">0</span>}
      </td>
      <td className="col-num col-drop">
        {review.biggest_drop_cp > 0
          ? <span className={
              review.biggest_drop_cp > 300 ? 'drop-blunder'
              : review.biggest_drop_cp > 150 ? 'drop-mistake'
              : 'drop-inaccuracy'
            }>⬇ {review.biggest_drop_cp}cp</span>
          : <span className="col-zero">—</span>}
      </td>
      <td className="col-phase">{review.critical_phase || '—'}</td>
    </tr>
  )
}

// ── Main component ────────────────────────────────────────────────────────────

export default function ReviewQueue({ username, appliedFilters = {}, onSelectGame }) {
  const [patterns, setPatterns]             = useState(null)
  const [patternsLoading, setPatternsLoading] = useState(true)
  const [reviews, setReviews]               = useState(null)
  const [reviewsLoading, setReviewsLoading] = useState(true)
  const [page, setPage]                     = useState(1)
  const [sortBy, setSortBy]                 = useState('blunder_count')
  const [sortDir, setSortDir]               = useState('desc')
  const [unreviewedCount, setUnreviewedCount] = useState(0)
  const [reviewRunning, setReviewRunning]   = useState(false)
  const [showReviewLog, setShowReviewLog]   = useState(false)

  const appliedFiltersRef = useRef(appliedFilters)
  const PAGE_SIZE = 50

  useEffect(() => { appliedFiltersRef.current = appliedFilters }, [appliedFilters])

  // ── fetch helpers ─────────────────────────────────────────────────────────

  const buildParams = useCallback((extra = {}) => {
    const af = appliedFiltersRef.current
    const p = new URLSearchParams()
    if (af.color)          p.set('color', af.color)
    if (af.outcome)        p.set('outcome', af.outcome)
    if (af.perf_type)      p.set('perf_type', af.perf_type)
    if (af.since_date)     p.set('since_date', af.since_date)
    if (af.until_date)     p.set('until_date', af.until_date)
    if (af.opening)        p.set('opening', af.opening)
    if (af.termination)    p.set('termination', af.termination)
    if (af.min_moves)      p.set('min_moves', af.min_moves)
    if (af.max_moves)      p.set('max_moves', af.max_moves)
    if (af.bookmarked_only) p.set('bookmarked_only', 'true')
    Object.entries(extra).forEach(([k, v]) => p.set(k, v))
    return p
  }, [])

  const fetchPatterns = useCallback(async () => {
    if (!username) return
    setPatternsLoading(true)
    try {
      const r = await fetch(`/api/mistake_patterns/${username}?${buildParams()}`)
      setPatterns(await r.json())
    } catch { setPatterns(null) }
    finally { setPatternsLoading(false) }
  }, [username, buildParams])

  const fetchReviews = useCallback(async (p = 1, sb = sortBy, sd = sortDir) => {
    if (!username) return
    setReviewsLoading(true)
    try {
      const params = buildParams({ page: p, page_size: PAGE_SIZE, sort_by: sb, sort_dir: sd })
      const r = await fetch(`/api/reviews/${username}?${params}`)
      setReviews(await r.json())
      setPage(p)
    } catch { setReviews(null) }
    finally { setReviewsLoading(false) }
  }, [username, sortBy, sortDir, buildParams])

  const fetchUnreviewed = useCallback(async () => {
    if (!username) return
    try {
      const r = await fetch(`/api/reviews/count/${username}`)
      const d = await r.json()
      setUnreviewedCount(d.count ?? 0)
    } catch {}
  }, [username])

  const fetchReviewStatus = useCallback(async () => {
    if (!username) return
    try {
      const r = await fetch(`/api/reviews/status/${username}`)
      const d = await r.json()
      setReviewRunning(d.running)
      setUnreviewedCount(d.unreviewed ?? 0)
    } catch {}
  }, [username])

  // ── effects ───────────────────────────────────────────────────────────────

  useEffect(() => {
    fetchPatterns()
    fetchReviews(1, sortBy, sortDir)
    fetchUnreviewed()
    fetchReviewStatus()
  }, [username, appliedFilters]) // eslint-disable-line react-hooks/exhaustive-deps

  // Poll review status when running
  useEffect(() => {
    if (!reviewRunning) return
    const id = setInterval(fetchReviewStatus, 3000)
    return () => clearInterval(id)
  }, [reviewRunning, fetchReviewStatus])

  const savedScrollY = useRef(null)

  // Restore scroll position after re-render caused by sorting
  useEffect(() => {
    if (savedScrollY.current !== null) {
      window.scrollTo({ top: savedScrollY.current, behavior: 'instant' })
      savedScrollY.current = null
    }
  }, [reviews])

  // ── sort handler ──────────────────────────────────────────────────────────

  const handleSort = (key) => {
    savedScrollY.current = window.scrollY
    let newDir = 'desc'
    if (sortBy === key) {
      newDir = sortDir === 'desc' ? 'asc' : 'desc'
    }
    setSortBy(key)
    setSortDir(newDir)
    fetchReviews(1, key, newDir)
  }

  // ── generate reviews ──────────────────────────────────────────────────────

  const handleGenerateReviews = async (force = false) => {
    const af = appliedFiltersRef.current
    try {
      const r = await fetch('/api/reviews/generate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          username,
          color:          af.color          || null,
          outcome:        af.outcome        || null,
          perf_type:      af.perf_type      || null,
          since_date:     af.since_date     || null,
          until_date:     af.until_date     || null,
          opening:        af.opening        || null,
          termination:    af.termination    || null,
          bookmarked_only: af.bookmarked_only || false,
          force,
        }),
      })
      const d = await r.json()
      if (d.started) setReviewRunning(true)
    } catch {}
    setShowReviewLog(true)
  }

  const handleReviewStatusUpdate = (status) => {
    setReviewRunning(status.running)
    if (!status.running) {
      // Refresh the grid once generation finishes
      fetchReviews(1, sortBy, sortDir)
      fetchUnreviewed()
    }
  }

  // ── row click → open game detail ─────────────────────────────────────────

  const handleRowSelect = async (review) => {
    try {
      const r = await fetch(`/api/game/${username}/${review.game_id}`)
      const fullGame = await r.json()
      onSelectGame(fullGame)
    } catch {
      onSelectGame(review)
    }
  }

  // ── render ────────────────────────────────────────────────────────────────

  const noEvals = patterns?.no_evals || reviews?.no_evals

  const reviewList  = reviews?.reviews ?? []
  const totalPages  = Math.ceil((reviews?.total ?? 0) / PAGE_SIZE)

  return (
    <div className="review-root">

      {/* ── Mistake Patterns ── */}
      <section className="patterns-section">
        <h3 className="section-title">Mistake Patterns</h3>
        {patternsLoading ? (
          <div className="review-loading">Loading patterns···</div>
        ) : noEvals ? (
          <div className="review-empty-inline">
            <div className="review-empty-icon">⚡</div>
            <div>
              <strong>No evaluations yet</strong>
              <div className="review-empty-sub">Run Stockfish evaluations first to unlock mistake patterns and reviews.</div>
            </div>
          </div>
        ) : patterns && !patterns.no_evals ? (
          <div className="patterns-grid">
            {/* Summary */}
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

      {/* ── Reviews Grid ── */}
      <section className="queue-section">
        <div className="queue-header">
          <h3 className="section-title">
            Reviews
            {reviews?.total != null && (
              <span className="queue-count"> ({reviews.total} reviewed)</span>
            )}
          </h3>
          <div className="queue-controls">
            {reviewRunning ? (
              <button className="btn-generate btn-generate-running" onClick={() => setShowReviewLog(true)}>
                ⟳ GENERATING…
              </button>
            ) : (
              <>
                {unreviewedCount > 0 && (
                  <button className="btn-generate" onClick={() => handleGenerateReviews(false)}>
                    ⟳ GENERATE ({unreviewedCount} new)
                  </button>
                )}
                {reviews?.total > 0 && (
                  <button className="btn-regenerate" onClick={() => handleGenerateReviews(true)}>
                    ↺ REGENERATE ALL
                  </button>
                )}
                {unreviewedCount === 0 && !reviews?.total && (
                  <button className="btn-generate btn-generate-done" onClick={() => setShowReviewLog(true)}>
                    ✓ ALL REVIEWED
                  </button>
                )}
              </>
            )}
          </div>
        </div>

        {reviewsLoading ? (
          <div className="review-loading">Loading reviews···</div>
        ) : !reviews || reviews.no_reviews ? (
          <div className="no-data-msg">
            No reviews yet. Click "Generate Reviews" to analyse your evaluated games.
          </div>
        ) : reviewList.length === 0 ? (
          <div className="no-data-msg">No reviews match the current filters.</div>
        ) : (
          <div className="reviews-table-wrap">
            <table className="reviews-table">
              <thead>
                <tr>
                  {COLUMNS.map(col => (
                    <th
                      key={col.key}
                      className={`col-${col.key} ${col.sortable ? 'sortable' : ''} ${sortBy === col.key ? 'sort-active' : ''}`}
                      onClick={col.sortable ? () => handleSort(col.key) : undefined}
                    >
                      {col.label}
                      {col.sortable && (
                        <SortIcon active={sortBy === col.key} dir={sortDir} />
                      )}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {reviewList.map(rev => (
                  <ReviewRow
                    key={rev.game_id}
                    review={rev}
                    username={username}
                    onSelect={handleRowSelect}
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}

        {totalPages > 1 && (
          <div className="queue-pagination">
            <button
              className="page-btn"
              disabled={page <= 1}
              onClick={() => fetchReviews(page - 1)}
            >←</button>
            <span className="page-info">{page} / {totalPages}</span>
            <button
              className="page-btn"
              disabled={page >= totalPages}
              onClick={() => fetchReviews(page + 1)}
            >→</button>
          </div>
        )}
      </section>

      {showReviewLog && (
        <ReviewLogModal
          username={username}
          onClose={() => setShowReviewLog(false)}
          onStatusUpdate={handleReviewStatusUpdate}
        />
      )}
    </div>
  )
}
