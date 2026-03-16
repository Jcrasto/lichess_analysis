import { useState } from 'react'
import BoardModal from './BoardModal.jsx'
import './GameDetail.css'

function HeaderRow({ label, value }) {
  if (!value || value === '?') return null
  return (
    <div className="hrow">
      <span className="hlabel">{label}</span>
      <span className="hval">{value}</span>
    </div>
  )
}

function injectEvals(movesStr, evals) {
  if (!evals || evals.length === 0) return movesStr
  // Build map from ply (1-based) to eval string
  const evalMap = {}
  for (const e of evals) {
    let evalStr
    if (e.mate_in !== null && e.mate_in !== undefined) {
      evalStr = `#${e.mate_in}`
    } else if (e.cp_score !== null && e.cp_score !== undefined) {
      evalStr = e.cp_score.toFixed(2)
    }
    if (evalStr) evalMap[e.move_number] = evalStr
  }

  // Tokenize moves and inject {[%eval ...]} after each move
  const tokens = movesStr.match(/(\d+\.)|(\.\.\.)|(\{[^}]*\})|([^\s]+)/g) || []
  const out = []
  let ply = 0
  for (let i = 0; i < tokens.length; i++) {
    const t = tokens[i]
    out.push(t)
    if (/^\d+\.$/.test(t) || /^\.\.\.$/.test(t)) continue
    if (['1-0', '0-1', '1/2-1/2', '*'].includes(t)) continue
    if (t.startsWith('{')) continue
    // It's a move token
    ply++
    const ev = evalMap[ply]
    if (ev) {
      // Only inject if next token is not already a comment
      const next = tokens[i + 1] || ''
      if (!next.startsWith('{')) {
        out.push(`{[%eval ${ev}]}`)
      }
    }
  }
  return out.join(' ')
}

function formatMoves(movesStr) {
  if (!movesStr) return []
  const tokens = movesStr.match(/(\d+\.)|(\.\.\.)|(\{[^}]*\})|([^\s]+)/g) || []
  const moves = []
  let i = 0
  while (i < tokens.length) {
    const t = tokens[i]
    if (/^\d+\.$/.test(t)) {
      const num = parseInt(t)
      const white = tokens[i + 1] || ''
      const whiteComment = (tokens[i + 2] || '').startsWith('{') ? tokens[i + 2] : null
      i += whiteComment ? 3 : 2

      let black = ''
      let blackComment = null
      if (i < tokens.length && !/^\d+/.test(tokens[i]) && !['1-0', '0-1', '1/2-1/2', '*'].includes(tokens[i])) {
        black = tokens[i]
        i++
        if (i < tokens.length && (tokens[i] || '').startsWith('{')) {
          blackComment = tokens[i]
          i++
        }
      }

      const parseEval = (comment) => {
        if (!comment) return null
        const m = comment.match(/\[%eval ([^\]]+)\]/)
        return m ? m[1] : null
      }

      moves.push({
        num,
        white: white.replace(/[+#?!]+$/, ''),
        whiteAnnotation: white.match(/[+#?!]+$/)?.[0] || '',
        whiteEval: parseEval(whiteComment),
        black: black.replace(/[+#?!]+$/, ''),
        blackAnnotation: black.match(/[+#?!]+$/)?.[0] || '',
        blackEval: parseEval(blackComment),
      })
    } else {
      i++
    }
  }
  return moves
}

function evalColor(ev) {
  if (!ev) return ''
  if (ev.startsWith('#')) return ev.includes('-') ? 'eval-loss' : 'eval-win'
  const n = parseFloat(ev)
  if (isNaN(n)) return ''
  if (n >= 1.5) return 'eval-win'
  if (n <= -1.5) return 'eval-loss'
  return 'eval-neutral'
}

export default function GameDetail({ game, username, evals, review, onClose, onMarkReviewed }) {
  const [expanded, setExpanded] = useState(false)
  const [markingReviewed, setMarkingReviewed] = useState(false)
  const [showBoard, setShowBoard] = useState(false)
  const movesWithEvals = injectEvals(game.moves, evals)
  const moves = formatMoves(movesWithEvals)
  const isWhite = game.white?.toLowerCase() === username?.toLowerCase()
  const lichessId = game.game_id

  return (
    <>
    <div className="detail-overlay" onClick={e => e.target === e.currentTarget && onClose()}>
      <div className={`detail-panel ${expanded ? 'detail-panel--expanded' : ''}`}>
        <div className="detail-header">
          <div>
            <div className="detail-players">
              <span className="dp white-p">{game.white} <em>({game.white_elo})</em></span>
              <span className="dp-vs">vs</span>
              <span className="dp black-p">{game.black} <em>({game.black_elo})</em></span>
            </div>
            <div className="detail-meta">
              {game.event} · {game.date} · {game.result}
            </div>
          </div>
          <div className="detail-header-actions">
            <button className="btn-board" onClick={() => setShowBoard(true)}>
              VIEW ON BOARD
            </button>
            {lichessId && (
              <a
                className="btn-lichess"
                href={`https://lichess.org/${lichessId}`}
                target="_blank"
                rel="noopener noreferrer"
              >
                VIEW ON LICHESS ↗
              </a>
            )}
            <button
              className="btn-expand"
              onClick={() => setExpanded(e => !e)}
              title={expanded ? 'Collapse panel' : 'Expand panel'}
            >{expanded ? '↙ Collapse' : '↖ Expand'}</button>
            <button className="modal-close" onClick={onClose}>✕</button>
          </div>
        </div>

        <div className="detail-body">
          <div className="detail-cols">
            <div className="detail-info">
              <div className="info-section">
                <div className="info-title">GAME INFO</div>
                <HeaderRow label="Opening" value={game.opening} />
                <HeaderRow label="ECO" value={game.eco} />
                <HeaderRow label="Termination" value={game.termination} />
                <HeaderRow label="Time Control" value={game.time_control} />
                <HeaderRow label="Date" value={game.date} />
              </div>
            </div>

            <div className="moves-pane">
              <div className="moves-title">MOVES{evals && evals.length > 0 ? ' (with evals)' : ''}</div>
              <div className="moves-grid">
                {moves.map((m, i) => (
                  <div key={i} className="move-row">
                    <span className="move-num">{m.num}.</span>
                    <span className="move-cell">
                      <span className="move-san">{m.white}</span>
                      {m.whiteAnnotation && <span className="move-ann">{m.whiteAnnotation}</span>}
                      {m.whiteEval && (
                        <span className={`move-eval ${evalColor(m.whiteEval)}`}>{m.whiteEval}</span>
                      )}
                    </span>
                    <span className="move-cell">
                      <span className="move-san">{m.black}</span>
                      {m.blackAnnotation && <span className="move-ann">{m.blackAnnotation}</span>}
                      {m.blackEval && (
                        <span className={`move-eval ${evalColor(m.blackEval)}`}>{m.blackEval}</span>
                      )}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          <div className="pgn-section">
            <div className="pgn-title">RAW PGN</div>
            <pre className="pgn-raw">{game.pgn}</pre>
          </div>

          {review && review.text_summary && (
            <div className="review-full-section">
              <div className="review-section-header">
                <div className="pgn-title">REVIEW</div>
                {onMarkReviewed && (
                  <button
                    className={`btn-mark-reviewed ${review.is_reviewed ? 'btn-mark-unreviewed' : ''}`}
                    disabled={markingReviewed}
                    onClick={async () => {
                      setMarkingReviewed(true)
                      await onMarkReviewed(game.game_id, !review.is_reviewed)
                      setMarkingReviewed(false)
                    }}
                  >
                    {review.is_reviewed ? 'Mark as Unreviewed' : 'Mark as Reviewed'}
                  </button>
                )}
              </div>
              <div className="review-badges">
                {review.blunder_count > 0 && (
                  <span className="badge badge-blunder">🔴 ×{review.blunder_count}</span>
                )}
                {review.mistake_count > 0 && (
                  <span className="badge badge-mistake">🟠 ×{review.mistake_count}</span>
                )}
                {review.inaccuracy_count > 0 && (
                  <span className="badge badge-inaccuracy">🟡 ×{review.inaccuracy_count}</span>
                )}
                {review.biggest_win_pct_drop > 0 && (
                  <span className={`drop-indicator ${
                    review.biggest_win_pct_drop > 20 ? 'drop-blunder'
                    : review.biggest_win_pct_drop > 10 ? 'drop-mistake'
                    : 'drop-inaccuracy'
                  }`}>⬇ {review.biggest_win_pct_drop.toFixed(1)}%</span>
                )}
                {review.lichess_accuracy_percentage > 0 && (
                  <span className={`drop-indicator ${
                    review.lichess_accuracy_percentage >= 80 ? 'acc-great'
                    : review.lichess_accuracy_percentage >= 60 ? 'acc-ok'
                    : 'acc-poor'
                  }`}>acc {review.lichess_accuracy_percentage.toFixed(1)}%</span>
                )}
              </div>
              <pre className="review-text">{review.text_summary}</pre>
            </div>
          )}
        </div>
      </div>
    </div>

    {showBoard && (
      <BoardModal
        game={game}
        username={username}
        evals={evals}
        review={review}
        onMarkReviewed={onMarkReviewed}
        onClose={() => setShowBoard(false)}
      />
    )}
    </>
  )
}
