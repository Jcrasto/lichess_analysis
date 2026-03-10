import { useEffect, useRef, useState } from 'react'
import './EvalLogModal.css'

export default function EvalLogModal({ username, onClose, onStatusUpdate }) {
  const [logs, setLogs] = useState([])
  const [status, setStatus] = useState(null)
  const [autoScroll, setAutoScroll] = useState(true)
  const [stopping, setStopping] = useState(false)
  const logsEndRef = useRef(null)
  const esRef = useRef(null)
  const scrollContainerRef = useRef(null)

  // Poll status every 3s
  useEffect(() => {
    const poll = async () => {
      try {
        const r = await fetch(`/api/evaluate/status/${username}`)
        const d = await r.json()
        setStatus(d)
        if (!d.running) setStopping(false)
        if (onStatusUpdate) onStatusUpdate(d)
      } catch {}
    }
    poll()
    const id = setInterval(poll, 3000)
    return () => clearInterval(id)
  }, [username, onStatusUpdate])

  // SSE log stream
  useEffect(() => {
    const es = new EventSource(`/api/evaluate/stream/${username}`)
    esRef.current = es

    es.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data)
        if (data.log) {
          setLogs(prev => [...prev, data.log])
        }
      } catch {}
    }

    es.onerror = () => {
      // Connection closed or error — SSE will auto-reconnect
    }

    return () => {
      es.close()
    }
  }, [username])

  // Auto-scroll
  useEffect(() => {
    if (autoScroll && logsEndRef.current) {
      logsEndRef.current.scrollIntoView({ behavior: 'smooth' })
    }
  }, [logs, autoScroll])

  const handleScroll = () => {
    const el = scrollContainerRef.current
    if (!el) return
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40
    setAutoScroll(atBottom)
  }

  const handleStop = async () => {
    setStopping(true)
    try {
      await fetch('/api/evaluate/stop', { method: 'POST' })
    } catch {}
    // stopping flag resets once status shows not running
  }

  const progress = status
    ? status.total_queued > 0
      ? Math.round(((status.evaluated + status.failed) / status.total_queued) * 100)
      : null
    : null

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="eval-log-modal" onClick={e => e.stopPropagation()}>
        <div className="eval-log-header">
          <div className="eval-log-title">
            {status?.running
              ? <span className="eval-status-running">⚡ EVALUATIONS RUNNING</span>
              : <span className="eval-status-done">✓ EVALUATIONS COMPLETE</span>
            }
          </div>
          <div className="eval-log-actions">
            {status?.running && (
              <button
                className="btn-stop-eval"
                onClick={handleStop}
                disabled={stopping}
                title="Stop after current game"
              >
                {stopping ? '⏳ STOPPING…' : '⏹ STOP'}
              </button>
            )}
            <button className="btn-modal-close" onClick={onClose}>✕</button>
          </div>
        </div>

        {status && (
          <div className="eval-stats">
            <div className="eval-stat">
              <span className="eval-stat-label">Evaluated</span>
              <span className="eval-stat-value green">{status.evaluated}</span>
            </div>
            <div className="eval-stat">
              <span className="eval-stat-label">Failed</span>
              <span className="eval-stat-value red">{status.failed}</span>
            </div>
            <div className="eval-stat">
              <span className="eval-stat-label">Remaining</span>
              <span className="eval-stat-value">{status.remaining}</span>
            </div>
            {status.total_queued > 0 && (
              <div className="eval-stat">
                <span className="eval-stat-label">Total</span>
                <span className="eval-stat-value">{status.total_queued}</span>
              </div>
            )}
          </div>
        )}

        {progress !== null && (
          <div className="eval-progress-bar">
            <div className="eval-progress-fill" style={{ width: `${progress}%` }} />
            <span className="eval-progress-label">{progress}%</span>
          </div>
        )}

        <div
          className="eval-log-body"
          ref={scrollContainerRef}
          onScroll={handleScroll}
        >
          {logs.length === 0 ? (
            <div className="eval-log-empty">Waiting for logs…</div>
          ) : (
            logs.map((line, i) => (
              <div key={i} className={`eval-log-line ${line.includes('✗') ? 'error' : line.includes('✓') ? 'success' : ''}`}>
                {line}
              </div>
            ))
          )}
          <div ref={logsEndRef} />
        </div>

        {!autoScroll && (
          <button
            className="btn-scroll-bottom"
            onClick={() => {
              setAutoScroll(true)
              logsEndRef.current?.scrollIntoView({ behavior: 'smooth' })
            }}
          >
            ↓ scroll to bottom
          </button>
        )}
      </div>
    </div>
  )
}
