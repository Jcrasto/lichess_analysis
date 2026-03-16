import { useState, useEffect, useMemo, useRef, useCallback } from 'react'
import './BoardModal.css'

// ── Constants ─────────────────────────────────────────────────────────────────

const PIECE_UNICODE = {
  K: '♔', Q: '♕', R: '♖', B: '♗', N: '♘', P: '♙',
  k: '♚', q: '♛', r: '♜', b: '♝', n: '♞', p: '♟',
}
const FILES = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
const STARTING_FEN = 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1'

// ── Helpers ───────────────────────────────────────────────────────────────────

function fenToBoard(fen) {
  return fen.split(' ')[0].split('/').map(row => {
    const arr = []
    for (const ch of row) {
      if (/\d/.test(ch)) { for (let i = 0; i < +ch; i++) arr.push(null) }
      else arr.push(ch)
    }
    return arr
  })
}

function winPct(cpPawns) {
  if (cpPawns == null) return 50
  return 50 + 50 * (2 / (1 + Math.exp(-0.00368208 * cpPawns * 100)) - 1)
}

function parsePlies(movesStr, evalsArr) {
  if (!movesStr) return []
  const evalMap = {}
  for (const e of evalsArr || []) {
    let s
    if (e.mate_in != null) s = `#${e.mate_in}`
    else if (e.cp_score != null) s = e.cp_score >= 0 ? `+${e.cp_score.toFixed(2)}` : e.cp_score.toFixed(2)
    if (s) evalMap[e.move_number] = s
  }

  const tokens = movesStr.match(/(\d+\.)|(\.\.\.)|(\{[^}]*\})|([^\s]+)/g) || []
  const plies = []
  let ply = 0
  for (let i = 0; i < tokens.length; i++) {
    const t = tokens[i]
    if (/^\d+\.$/.test(t) || /^\.\.\.$/.test(t)) continue
    if (['1-0', '0-1', '1/2-1/2', '*'].includes(t)) continue
    if (t.startsWith('{')) continue
    ply++
    plies.push({
      ply,
      num: Math.ceil(ply / 2),
      side: ply % 2 === 1 ? 'w' : 'b',
      san: t,
      eval: evalMap[ply] || null,
      isBestLine: false,
    })
  }
  return plies
}

// ── Story section parser ──────────────────────────────────────────────────────

function parseStorySections(text) {
  if (!text) return { sections: [], dividers: [] }
  const lines = text.split('\n')
  const sections = []
  const dividers = []
  let pending = []
  let sectionStart = 0

  for (const line of lines) {
    const m = line.match(/── After move (\d+)/)
    if (m) {
      const cpMove = parseInt(m[1])
      sections.push({ lines: pending, startMove: sectionStart, endMove: cpMove })
      dividers.push({ line, afterMove: cpMove })
      pending = []
      sectionStart = cpMove + 1
    } else {
      pending.push(line)
    }
  }
  sections.push({ lines: pending, startMove: sectionStart, endMove: Infinity })
  return { sections, dividers }
}

function evalColorClass(ev) {
  if (!ev) return ''
  if (ev.startsWith('#')) return ev.startsWith('#-') ? 'ev-loss' : 'ev-win'
  const n = parseFloat(ev)
  if (isNaN(n)) return ''
  if (n >= 1.5) return 'ev-win'
  if (n <= -1.5) return 'ev-loss'
  return 'ev-neutral'
}

// ── Eval Bar ──────────────────────────────────────────────────────────────────

function EvalBar({ cpScore, mateIn, flipped }) {
  let whiteWin
  if (mateIn != null) {
    whiteWin = mateIn > 0 ? 99 : 1
  } else {
    whiteWin = winPct(cpScore)
  }
  const blackWin = 100 - whiteWin

  let label
  if (mateIn != null) label = `M${Math.abs(mateIn)}`
  else if (cpScore != null) label = `${cpScore >= 0 ? '+' : ''}${cpScore.toFixed(2)}`
  else label = '0.00'

  const topPct  = flipped ? whiteWin : blackWin
  const botPct  = flipped ? blackWin : whiteWin
  const topDark = !flipped

  return (
    <div className="eval-bar">
      <div className={`eval-seg ${topDark ? 'eval-seg-dark' : 'eval-seg-light'}`} style={{ flex: topPct }} />
      <div className={`eval-seg ${topDark ? 'eval-seg-light' : 'eval-seg-dark'}`} style={{ flex: botPct }}>
        <span className={`eval-bar-label ${topDark ? 'ebl-dark' : 'ebl-light'}`}>{label}</span>
      </div>
    </div>
  )
}

// ── Chess Board ───────────────────────────────────────────────────────────────

function Board({ fen, flipped }) {
  const board = fenToBoard(fen)

  const rankRows = []
  for (let displayRow = 0; displayRow < 8; displayRow++) {
    const rank = flipped ? displayRow + 1 : 8 - displayRow
    const gridRow = 8 - rank
    const cells = []
    for (let displayCol = 0; displayCol < 8; displayCol++) {
      const fileIdx = flipped ? 7 - displayCol : displayCol
      const piece = board[gridRow]?.[fileIdx] ?? null
      const isLight = (rank + fileIdx) % 2 === 0
      cells.push(
        <div key={displayCol} className={`sq ${isLight ? 'sq-l' : 'sq-d'}`}>
          {piece && (
            <span className={`piece ${piece === piece.toUpperCase() ? 'pc-w' : 'pc-b'}`}>
              {PIECE_UNICODE[piece]}
            </span>
          )}
        </div>
      )
    }
    rankRows.push(
      <div key={rank} className="board-rank-row">
        <span className="rank-lbl">{rank}</span>
        {cells}
      </div>
    )
  }

  const displayFiles = flipped ? [...FILES].reverse() : FILES

  return (
    <div className="chess-board">
      {rankRows}
      <div className="file-label-row">
        <span className="rank-lbl-spacer" />
        {displayFiles.map(f => <span key={f} className="file-lbl">{f}</span>)}
      </div>
    </div>
  )
}

// ── Move List ─────────────────────────────────────────────────────────────────

function MoveList({ plies, currentPly, onSelect }) {
  const activeRef = useRef(null)

  useEffect(() => {
    activeRef.current?.scrollIntoView({ block: 'nearest', behavior: 'smooth' })
  }, [currentPly])

  const rows = useMemo(() => {
    const r = []
    for (let i = 0; i < plies.length; i += 2) {
      const w = plies[i], b = plies[i + 1]
      r.push({
        num: w.num,
        wPly: w.ply, wSan: w.san, wEval: w.eval, wBL: w.isBestLine,
        bPly: b?.ply, bSan: b?.san, bEval: b?.eval, bBL: b?.isBestLine,
      })
    }
    return r
  }, [plies])

  return (
    <div className="move-list">
      {rows.map(row => {
        const wActive = currentPly === row.wPly
        const bActive = currentPly === row.bPly
        return (
          <div key={row.num} className="ml-row">
            <span className="ml-num">{row.num}.</span>
            <span
              ref={wActive ? activeRef : null}
              className={`ml-cell ${wActive ? 'ml-active' : ''} ${row.wBL ? 'ml-best-line' : ''}`}
              onClick={() => onSelect(row.wPly)}
            >
              <span className="ml-san">{row.wSan}</span>
              {row.wEval && <span className={`ml-eval ${evalColorClass(row.wEval)}`}>{row.wEval}</span>}
            </span>
            <span
              ref={bActive ? activeRef : null}
              className={`ml-cell ${bActive ? 'ml-active' : ''} ${row.bBL ? 'ml-best-line' : ''}`}
              onClick={() => row.bPly != null && onSelect(row.bPly)}
            >
              <span className="ml-san">{row.bSan || ''}</span>
              {row.bEval && <span className={`ml-eval ${evalColorClass(row.bEval)}`}>{row.bEval}</span>}
            </span>
          </div>
        )
      })}
    </div>
  )
}

// ── Main Modal ────────────────────────────────────────────────────────────────

export default function BoardModal({ game, username, evals, review, onMarkReviewed, onClose }) {
  const isUserWhite = game.white?.toLowerCase() === username?.toLowerCase()
  const [ply, setPly]       = useState(0)
  const [flipped, setFlipped] = useState(!isUserWhite)
  const [markingReviewed, setMarkingReviewed] = useState(false)
  const [isReviewed, setIsReviewed] = useState(review?.is_reviewed ?? false)

  // Best line state
  const [bestLine, setBestLine]             = useState(null)   // [{san, uci, fen}] or null
  const [bestLineFromPly, setBestLineFromPly] = useState(null) // ply at which game diverges into best line
  const [bestLineLoading, setBestLineLoading] = useState(false)

  const fenMap = useMemo(() => {
    const m = { 0: STARTING_FEN }
    for (const e of evals || []) {
      if (e.fen && e.move_number != null) m[e.move_number] = e.fen
    }
    return m
  }, [evals])

  const evalMap = useMemo(() => {
    const m = {}
    for (const e of evals || []) {
      if (e.move_number != null) m[e.move_number] = e
    }
    return m
  }, [evals])

  const plies = useMemo(() => parsePlies(game.moves, evals), [game.moves, evals])

  // Display plies: game moves up to (not including) bestLineFromPly, then best line plies
  const displayPlies = useMemo(() => {
    if (!bestLine || bestLineFromPly == null) return plies
    const gamePart = plies.slice(0, bestLineFromPly - 1)
    const blPart = bestLine.map((move, i) => ({
      ply: bestLineFromPly + i,
      num: Math.ceil((bestLineFromPly + i) / 2),
      side: (bestLineFromPly + i) % 2 === 1 ? 'w' : 'b',
      san: move.san,
      eval: null,
      isBestLine: true,
    }))
    return [...gamePart, ...blPart]
  }, [plies, bestLine, bestLineFromPly])

  // Display FEN map: extend with best line FENs
  const displayFenMap = useMemo(() => {
    if (!bestLine || bestLineFromPly == null) return fenMap
    const extended = { ...fenMap }
    bestLine.forEach((move, i) => {
      extended[bestLineFromPly + i] = move.fen
    })
    return extended
  }, [fenMap, bestLine, bestLineFromPly])

  const maxPly = displayPlies.length
  const currentFen = displayFenMap[ply] || STARTING_FEN
  const currentEvalEntry = (bestLine && bestLineFromPly != null && ply >= bestLineFromPly)
    ? null
    : (evalMap[ply] || null)

  // Story sections
  const { sections: storySections, dividers: storyDividers } = useMemo(
    () => parseStorySections(review?.text_summary),
    [review]
  )
  const currentFullMove = ply > 0 ? Math.ceil(ply / 2) : 0
  const activeSectionIdx = storySections.findIndex(
    s => currentFullMove >= s.startMove && currentFullMove <= s.endMove
  )
  const activeSectionRef = useRef(null)
  useEffect(() => {
    activeSectionRef.current?.scrollIntoView({ block: 'nearest', behavior: 'smooth' })
  }, [activeSectionIdx])

  const prev = useCallback(() => setPly(p => Math.max(0, p - 1)), [])
  const next = useCallback(() => setPly(p => Math.min(maxPly, p + 1)), [maxPly])

  useEffect(() => {
    const handler = e => {
      if (e.key === 'ArrowLeft')  { e.preventDefault(); prev() }
      if (e.key === 'ArrowRight') { e.preventDefault(); next() }
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [prev, next, onClose])

  // Use displayPlies for the move label
  const info = ply > 0 ? displayPlies[ply - 1] : null
  const moveLabel = info
    ? `Move ${info.num}${info.side === 'w' ? '.' : '...'} ${info.san}${info.isBestLine ? ' ★' : ''}`
    : 'Starting position'

  // ── Best line handlers ────────────────────────────────────────────────────

  const handleShowBestLine = useCallback(async () => {
    const positionFen = fenMap[ply - 1] || STARTING_FEN
    setBestLineLoading(true)
    try {
      const r = await fetch('/api/analyze_position', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ fen: positionFen, depth: 15, moves: 6 }),
      })
      const d = await r.json()
      if (d.pv && d.pv.length > 0) {
        setBestLine(d.pv)
        setBestLineFromPly(ply)
      }
    } catch (e) {
      console.error('Best line fetch failed:', e)
    } finally {
      setBestLineLoading(false)
    }
  }, [ply, fenMap])

  const handleReturnToGame = useCallback(() => {
    if (bestLineFromPly != null && ply >= bestLineFromPly) {
      setPly(bestLineFromPly - 1)
    }
    setBestLine(null)
    setBestLineFromPly(null)
  }, [ply, bestLineFromPly])

  return (
    <div className="bm-overlay" onClick={e => e.target === e.currentTarget && onClose()}>
      <div className="bm-modal">

        {/* ── Header ── */}
        <div className="bm-header">
          <div className="bm-title">
            <span className="bm-player-w">{game.white} ({game.white_elo})</span>
            <span className="bm-vs"> vs </span>
            <span className="bm-player-b">{game.black} ({game.black_elo})</span>
            {game.opening && <span className="bm-opening"> · {game.opening}</span>}
            {game.date && <span className="bm-opening"> · {game.date}</span>}
          </div>
          <div className="bm-hactions">
            <button className="bm-btn" onClick={() => setFlipped(f => !f)}>⇅ Flip</button>
            {review && onMarkReviewed && (
              <button
                className={`bm-btn bm-reviewed-btn${isReviewed ? ' bm-reviewed-btn--done' : ''}`}
                disabled={markingReviewed}
                onClick={async () => {
                  setMarkingReviewed(true)
                  await onMarkReviewed(game.game_id, !isReviewed)
                  setIsReviewed(v => !v)
                  setMarkingReviewed(false)
                }}
              >
                {isReviewed ? '✓ Reviewed' : 'Mark Reviewed'}
              </button>
            )}
            {game.game_id && (
              <a
                className="bm-btn"
                href={`https://lichess.org/${game.game_id}`}
                target="_blank"
                rel="noopener noreferrer"
              >
                VIEW ON LICHESS ↗
              </a>
            )}
            <button className="bm-btn bm-close-btn" onClick={onClose}>✕</button>
          </div>
        </div>

        {/* ── Body ── */}
        <div className="bm-body">

          {/* Board + nav */}
          <div className="bm-board-section">
            <div className="bm-board-row">
              <EvalBar
                cpScore={currentEvalEntry?.cp_score}
                mateIn={currentEvalEntry?.mate_in}
                flipped={flipped}
              />
              <Board fen={currentFen} flipped={flipped} />
            </div>

            <div className="bm-nav">
              <button className="bm-nav-btn" onClick={() => setPly(0)} disabled={ply === 0} title="Start">⏮</button>
              <button className="bm-nav-btn" onClick={prev}           disabled={ply === 0} title="Previous (←)">◀</button>
              <span className={`bm-move-label${bestLine && ply >= (bestLineFromPly ?? Infinity) ? ' bm-move-label--best' : ''}`}>
                {moveLabel}
              </span>
              <button className="bm-nav-btn" onClick={next}           disabled={ply >= maxPly} title="Next (→)">▶</button>
              <button className="bm-nav-btn" onClick={() => setPly(maxPly)} disabled={ply >= maxPly} title="End">⏭</button>
            </div>
          </div>

          {/* Right panel */}
          <div className="bm-right">
            <div className="bm-moves-wrap">
              <div className="bm-panel-title">
                MOVES
                {bestLine && <span className="bm-panel-best-badge"> · BEST LINE FROM MOVE {bestLineFromPly != null ? Math.ceil(bestLineFromPly / 2) : ''}</span>}
              </div>
              <div className="ml-player-header">
                <span className="ml-num-spacer" />
                <span className="ml-player-white">{game.white}</span>
                <span className="ml-player-black">{game.black}</span>
              </div>
              <MoveList plies={displayPlies} currentPly={ply} onSelect={setPly} />

              {/* Best line bar */}
              <div className="bm-best-line-bar">
                {!bestLine && ply > 0 && (
                  <button
                    className="bm-best-line-btn"
                    onClick={handleShowBestLine}
                    disabled={bestLineLoading}
                  >
                    {bestLineLoading ? '⟳ ANALYZING...' : '⚡ SHOW BEST LINE'}
                  </button>
                )}
                {bestLine && (
                  <button className="bm-return-btn" onClick={handleReturnToGame}>
                    ↩ RETURN TO GAME
                  </button>
                )}
              </div>
            </div>

            {review?.text_summary && (
              <div className="bm-story-wrap">
                <div className="bm-panel-title">GAME STORY</div>
                <div className="bm-story-scroll">
                  {storySections.map((section, i) => (
                    <div key={i}>
                      <div
                        ref={i === activeSectionIdx ? activeSectionRef : null}
                        className={`story-section${i === activeSectionIdx ? ' story-section-active' : ''}`}
                      >
                        <pre className="story-section-text">{section.lines.join('\n')}</pre>
                      </div>
                      {storyDividers[i] && (
                        <pre className="story-divider">{storyDividers[i].line}</pre>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
