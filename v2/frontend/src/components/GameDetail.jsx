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

function formatMoves(movesStr) {
  if (!movesStr) return []
  // Split into tokens: move numbers, moves, comments, result
  const tokens = movesStr.match(/(\d+\.)|(\.\.\.)|(\{[^}]*\})|([^\s]+)/g) || []
  const moves = []
  let i = 0
  while (i < tokens.length) {
    const t = tokens[i]
    if (/^\d+\.$/.test(t)) {
      // Move number
      const num = parseInt(t)
      const white = tokens[i + 1] || ''
      const whiteComment = (tokens[i + 2] || '').startsWith('{') ? tokens[i + 2] : null
      i += whiteComment ? 3 : 2

      // Check for black move
      let black = ''
      let blackComment = null
      if (i < tokens.length && !/^\d+/.test(tokens[i]) && !['1-0','0-1','1/2-1/2','*'].includes(tokens[i])) {
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

export default function GameDetail({ game, username, onClose }) {
  const h = game.headers || {}
  const moves = formatMoves(game.moves)
  const isWhite = h.White?.toLowerCase() === username?.toLowerCase()
  const lichessId = (h.Site || '').split('/').pop()

  return (
    <div className="detail-overlay" onClick={e => e.target === e.currentTarget && onClose()}>
      <div className="detail-panel">
        <div className="detail-header">
          <div>
            <div className="detail-players">
              <span className="dp white-p">{h.White} <em>({h.WhiteElo})</em></span>
              <span className="dp-vs">vs</span>
              <span className="dp black-p">{h.Black} <em>({h.BlackElo})</em></span>
            </div>
            <div className="detail-meta">
              {h.Event} · {h.Date} · {h.Result}
            </div>
          </div>
          <div className="detail-header-actions">
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
            <button className="modal-close" onClick={onClose}>✕</button>
          </div>
        </div>

        <div className="detail-body">
          <div className="detail-cols">
            <div className="detail-info">
              <div className="info-section">
                <div className="info-title">GAME INFO</div>
                <HeaderRow label="Opening" value={h.Opening} />
                <HeaderRow label="ECO" value={h.ECO} />
                <HeaderRow label="Termination" value={h.Termination} />
                <HeaderRow label="Time Control" value={h.TimeControl} />
                <HeaderRow label="Date" value={h.Date} />
                <HeaderRow label="White Clock" value={h.WhiteClock} />
                <HeaderRow label="Black Clock" value={h.BlackClock} />
              </div>
            </div>

            <div className="moves-pane">
              <div className="moves-title">MOVES</div>
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
        </div>
      </div>
    </div>
  )
}
