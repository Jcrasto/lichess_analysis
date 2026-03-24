import { useState } from 'react'
import './RefreshModal.css'

export default function RefreshModal({ username, lastDate, onClose, onDone }) {
  const [loading, setLoading] = useState(false)
  const [loadingType, setLoadingType] = useState(null)
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)

  const handleRefresh = async (type) => {
    setLoading(true)
    setLoadingType(type)
    setError(null)
    setResult(null)
    try {
      const r = await fetch(`/api/refresh/${type}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username }),
      })
      const d = await r.json()
      if (!r.ok) throw new Error(d.detail || 'Refresh failed')
      setResult({ ...d, type })
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
      setLoadingType(null)
    }
  }

  return (
    <div className="modal-overlay" onClick={e => !loading && e.target === e.currentTarget && onClose()}>
      <div className="modal">
        <div className="modal-header">
          <div>
            <div className="modal-title">REFRESH DATA</div>
            <div className="modal-sub">Downloads game history from Lichess — no analysis is run</div>
          </div>
          <button className="modal-close" onClick={onClose} disabled={loading}>✕</button>
        </div>

        <div className="modal-body">
          {error && <div className="modal-error">{error}</div>}

          {loading ? (
            <div className="refresh-loading">
              <div className="refresh-loading-spinner" />
              <div className="refresh-loading-title">
                {loadingType === 'full' ? 'DOWNLOADING ALL GAMES FROM LICHESS' : 'DOWNLOADING NEW GAMES FROM LICHESS'}
              </div>
              <div className="refresh-loading-sub">
                This may take a few minutes for large game archives. Do not close this window.
              </div>
            </div>
          ) : result ? (
            <div className="modal-result">
              <div className="result-line">
                <span className="result-key">FETCHED</span>
                <span className="result-val">{result.fetched}</span>
              </div>
              {result.type === 'incremental' ? (
                <>
                  <div className="result-line">
                    <span className="result-key">NEW GAMES</span>
                    <span className="result-val accent">{result.new_games}</span>
                  </div>
                  <div className="result-line">
                    <span className="result-key">MONTHS UPDATED</span>
                    <span className="result-val">{result.months_updated}</span>
                  </div>
                </>
              ) : (
                <div className="result-line">
                  <span className="result-key">MONTHS WRITTEN</span>
                  <span className="result-val">{result.months_written}</span>
                </div>
              )}
              <div className="result-line">
                <span className="result-key">TOTAL GAMES</span>
                <span className="result-val">{result.total_games}</span>
              </div>
            </div>
          ) : (
            <div className="refresh-options">
              <button
                className="btn-refresh-option"
                onClick={() => handleRefresh('incremental')}
                disabled={loading}
              >
                <span className="refresh-option-title">⟳ REFRESH SINCE {lastDate || 'LAST GAME'}</span>
                <span className="refresh-option-sub">Only download games newer than your last stored game</span>
              </button>
              <button
                className="btn-refresh-option secondary"
                onClick={() => handleRefresh('full')}
                disabled={loading}
              >
                <span className="refresh-option-title">↺ FULL REFRESH (ALL GAMES)</span>
                <span className="refresh-option-sub">Download entire game history from Lichess and rebuild storage</span>
              </button>
            </div>
          )}
        </div>

        <div className="modal-footer">
          {!loading && <button className="btn-ghost" onClick={onClose}>CANCEL</button>}
          {result && (
            <button className="btn-primary" onClick={() => onDone(result)}>DONE</button>
          )}
        </div>
      </div>
    </div>
  )
}
