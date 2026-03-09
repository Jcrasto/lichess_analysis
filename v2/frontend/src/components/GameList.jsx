import './GameList.css'

function getResult(result, username, white, black) {
  if (!result) return { text: '?', cls: '' }
  const isWhite = white?.toLowerCase() === username?.toLowerCase()
  if (result === '1-0') return isWhite ? { text: 'Win', cls: 'win' } : { text: 'Loss', cls: 'loss' }
  if (result === '0-1') return isWhite ? { text: 'Loss', cls: 'loss' } : { text: 'Win', cls: 'win' }
  if (result === '1/2-1/2') return { text: 'Draw', cls: 'draw' }
  return { text: result, cls: '' }
}

function getColor(username, white) {
  return white?.toLowerCase() === username?.toLowerCase() ? 'white' : 'black'
}

export default function GameList({ games, username, loading, onSelect }) {
  if (loading) {
    return (
      <div className="game-list-loading">
        <span>Loading games···</span>
      </div>
    )
  }

  return (
    <div className="game-list">
      {games.map((game, i) => {
        const h = game.headers || {}
        const color = getColor(username, h.White)
        const r = getResult(h.Result, username, h.White, h.Black)
        const opponent = color === 'white' ? h.Black : h.White
        const opponentElo = color === 'white' ? h.BlackElo : h.WhiteElo
        const eventShort = (h.Event || '').replace(/^Rated /, '').replace(/ game$/, '')

        return (
          <div key={i} className="game-row" onClick={() => onSelect(game)}>
            <div className={`color-pip ${color}`} title={`Played as ${color}`} />
            <div className="game-result-col">
              <span className={`result-badge ${r.cls}`}>{r.text}</span>
            </div>
            <div className="game-players-col">
              <span className="player-name">{opponent || '?'}</span>
              {opponentElo && opponentElo !== '?' && (
                <span className="player-elo">{opponentElo}</span>
              )}
            </div>
            <div className="game-type-col">
              <span className="game-type">{eventShort}</span>
              {h.ECO && h.ECO !== '?' && <span className="game-eco">{h.ECO}</span>}
            </div>
            <div className="game-opening-col">
              <span className="game-opening">{h.Opening || ''}</span>
            </div>
            <div className="game-date-col">
              <span className="game-date">{(h.Date || '').replace(/\.\?\?$/, '')}</span>
            </div>
          </div>
        )
      })}
    </div>
  )
}
