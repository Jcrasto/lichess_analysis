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
  // Returns grid[rank8..rank1][fileA..fileH], null = empty square
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
  // Build eval map from evals
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
    })
  }
  return plies
}

// ── Story section parser ──────────────────────────────────────────────────────

function parseStorySections(text) {
  if (!text) return { sections: [], dividers: [] }
  const lines = text.split('\n')
  const sections = []   // { lines[], startMove, endMove }
  const dividers = []   // { line, afterMove }
  let pending = []
  let sectionStart = 0  // first move covered by pending section

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
  // Trailing section after last checkpoint
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

  // flipped = white is at top of board
  const topPct  = flipped ? whiteWin : blackWin
  const botPct  = flipped ? blackWin : whiteWin
  const topDark = !flipped  // top section is dark (black) when not flipped

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

  // Group into pairs: {num, wPly, wSan, wEval, bPly, bSan, bEval}
  const rows = useMemo(() => {
    const r = []
    for (let i = 0; i < plies.length; i += 2) {
      const w = plies[i], b = plies[i + 1]
      r.push({ num: w.num, wPly: w.ply, wSan: w.san, wEval: w.eval, bPly: b?.ply, bSan: b?.san, bEval: b?.eval })
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
              className={`ml-cell ${wActive ? 'ml-active' : ''}`}
              onClick={() => onSelect(row.wPly)}
            >
              <span className="ml-san">{row.wSan}</span>
              {row.wEval && <span className={`ml-eval ${evalColorClass(row.wEval)}`}>{row.wEval}</span>}
            </span>
            <span
              ref={bActive ? activeRef : null}
              className={`ml-cell ${bActive ? 'ml-active' : ''}`}
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

export default function BoardModal({ game, username, evals, review, onClose }) {
  const isUserWhite = game.white?.toLowerCase() === username?.toLowerCase()
  const [ply, setPly]       = useState(0)
  const [flipped, setFlipped] = useState(!isUserWhite)

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
  const maxPly = plies.length

  const currentFen       = fenMap[ply] || STARTING_FEN
  const currentEvalEntry = evalMap[ply] || null

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

  const info = ply > 0 ? plies[ply - 1] : null
  const moveLabel = info
    ? `Move ${info.num}${info.side === 'w' ? '.' : '...'} ${info.san}`
    : 'Starting position'

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
              <button className="bm-nav-btn" onClick={() => setPly(0)} disabled={ply === 0} title="Start (Home)">⏮</button>
              <button className="bm-nav-btn" onClick={prev}           disabled={ply === 0} title="Previous (←)">◀</button>
              <span className="bm-move-label">{moveLabel}</span>
              <button className="bm-nav-btn" onClick={next}           disabled={ply >= maxPly} title="Next (→)">▶</button>
              <button className="bm-nav-btn" onClick={() => setPly(maxPly)} disabled={ply >= maxPly} title="End">⏭</button>
            </div>
          </div>

          {/* Right panel */}
          <div className="bm-right">
            <div className="bm-moves-wrap">
              <div className="bm-panel-title">MOVES</div>
              <MoveList plies={plies} currentPly={ply} onSelect={setPly} />
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
