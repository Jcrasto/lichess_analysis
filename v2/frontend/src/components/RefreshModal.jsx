import { useState } from 'react'
import './RefreshModal.css'

const PERF_TYPES = ['', 'bullet', 'blitz', 'rapid', 'classical', 'correspondence', 'puzzle']

export default function RefreshModal({ defaultUsername, onClose, onDone }) {
  const [username, setUsername] = useState(defaultUsername || '')
  const [since, setSince] = useState('')
  const [until, setUntil] = useState('')
  const [max, setMax] = useState(100)
  const [perfType, setPerfType] = useState('')
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)

  const handleRefresh = async () => {
    if (!username.trim()) return
    setLoading(true)
    setError(null)
    setResult(null)
    try {
      const body = {
        username: username.trim(),
        max,
        ...(since && { since }),
        ...(until && { until }),
        ...(perfType && { perf_type: perfType }),
      }
      const r = await fetch('/api/refresh', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const d = await r.json()
      if (!r.ok) throw new Error(d.detail || 'Refresh failed')
      setResult(d)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="modal-overlay" onClick={e => e.target === e.currentTarget && onClose()}>
      <div className="modal">
        <div className="modal-header">
          <div>
            <div className="modal-title">REFRESH USER DATA</div>
            <div className="modal-sub">Fetch games from the Lichess API</div>
          </div>
          <button className="modal-close" onClick={onClose}>✕</button>
        </div>

        <div className="modal-body">
          <div className="field">
            <label className="field-label">USERNAME</label>
            <input
              className="text-input"
              value={username}
              onChange={e => setUsername(e.target.value)}
              placeholder="Lichess username"
            />
          </div>

          <div className="field-row">
            <div className="field">
              <label className="field-label">SINCE</label>
              <input
                type="date"
                className="text-input"
                value={since}
                onChange={e => setSince(e.target.value)}
              />
            </div>
            <div className="field">
              <label className="field-label">UNTIL</label>
              <input
                type="date"
                className="text-input"
                value={until}
                onChange={e => setUntil(e.target.value)}
              />
            </div>
          </div>

          <div className="field-row">
            <div className="field">
              <label className="field-label">MAX GAMES</label>
              <input
                type="number"
                className="text-input"
                value={max}
                min={1}
                max={500}
                onChange={e => setMax(Number(e.target.value))}
              />
            </div>
            <div className="field">
              <label className="field-label">GAME TYPE</label>
              <select
                className="select-input"
                value={perfType}
                onChange={e => setPerfType(e.target.value)}
              >
                {PERF_TYPES.map(t => (
                  <option key={t} value={t}>{t || 'All Types'}</option>
                ))}
              </select>
            </div>
          </div>

          {error && <div className="modal-error">{error}</div>}

          {result && (
            <div className="modal-result">
              <div className="result-line">
                <span className="result-key">FETCHED</span>
                <span className="result-val">{result.fetched}</span>
              </div>
              <div className="result-line">
                <span className="result-key">NEW ADDED</span>
                <span className="result-val accent">{result.added}</span>
              </div>
              <div className="result-line">
                <span className="result-key">TOTAL STORED</span>
                <span className="result-val">{result.total}</span>
              </div>
            </div>
          )}
        </div>

        <div className="modal-footer">
          <button className="btn-ghost" onClick={onClose}>CANCEL</button>
          {result ? (
            <button className="btn-primary" onClick={() => onDone(result)}>DONE</button>
          ) : (
            <button
              className="btn-primary"
              onClick={handleRefresh}
              disabled={loading || !username.trim()}
            >
              {loading ? (
                <span className="spinner-text">FETCHING···</span>
              ) : '⟳ FETCH GAMES'}
            </button>
          )}
        </div>
      </div>
    </div>
  )
}
