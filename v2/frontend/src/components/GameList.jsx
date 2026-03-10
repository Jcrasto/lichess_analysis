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

export default function GameList({ games, username, loading, onSelect, bookmarks = new Set() }) {
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
        const color = getColor(username, game.white)
        const r = getResult(game.result, username, game.white, game.black)
        const opponent = color === 'white' ? game.black : game.white
        const opponentElo = color === 'white' ? game.black_elo : game.white_elo
        const eventShort = (game.event || '').replace(/^Rated /, '').replace(/ game$/, '')

        return (
          <div key={game.game_id || i} className="game-row" onClick={() => onSelect(game)}>
            <div className={`color-badge ${color}`}>{color === 'white' ? 'W' : 'B'}</div>
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
              {game.eco && game.eco !== '?' && <span className="game-eco">{game.eco}</span>}
            </div>
            <div className="game-opening-col">
              <span className="game-opening">{game.opening || ''}</span>
            </div>
            <div className="game-date-col">
              {bookmarks.has(game.game_id) && <span className="bookmark-star" title="Bookmarked on Lichess">★</span>}
              {game.has_eval && <span className="eval-dot" title="Has engine evaluation">⚡</span>}
              <span className="game-date">{(game.date || '').replace(/\.\?\?$/, '')}</span>
            </div>
          </div>
        )
      })}
    </div>
  )
}
