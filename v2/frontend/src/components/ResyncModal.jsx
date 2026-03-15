import { useEffect, useRef, useState } from 'react'
import './RefreshModal.css'
import './EvalLogModal.css'
import './ResyncModal.css'

const STEPS = [
  { key: 'refresh',  label: 'Fetch latest games from Lichess' },
  { key: 'evaluate', label: 'Run Stockfish evaluations' },
  { key: 'review',   label: 'Generate game reviews' },
]

// Pure display component — orchestration lives in App.jsx
export default function ResyncModal({
  username, appliedFilters,
  started, currentStep, error, refreshResult,
  onStart, onClose,
}) {
  const [logs, setLogs]             = useState([])
  const [autoScroll, setAutoScroll] = useState(true)
  const [stepStatus, setStepStatus] = useState(null)
  const logsEndRef         = useRef(null)
  const scrollContainerRef = useRef(null)

  const filters = {
    since_date: appliedFilters.since_date || null,
    until_date: appliedFilters.until_date || null,
    color:      appliedFilters.color      || null,
    outcome:    appliedFilters.outcome    || null,
    perf_type:  appliedFilters.perf_type  || null,
    opening:    appliedFilters.opening    || null,
  }

  // SSE stream — reconnect whenever the active step changes to 1 or 2
  useEffect(() => {
    if (!started || (currentStep !== 1 && currentStep !== 2)) return
    setLogs([])
    setStepStatus(null)
    const url = currentStep === 1
      ? `/api/evaluate/stream/${username}`
      : `/api/reviews/stream/${username}`
    const es = new EventSource(url)
    es.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data)
        if (data.log) setLogs(prev => [...prev, data.log])
      } catch {}
    }
    return () => es.close()
  }, [started, currentStep, username])

  // Status polling for stats display
  useEffect(() => {
    if (!started || (currentStep !== 1 && currentStep !== 2)) return
    const statusUrl = currentStep === 1
      ? `/api/evaluate/status/${username}`
      : `/api/reviews/status/${username}`
    const poll = async () => {
      try { setStepStatus(await (await fetch(statusUrl)).json()) } catch {}
    }
    poll()
    const id = setInterval(poll, 2000)
    return () => clearInterval(id)
  }, [started, currentStep, username])

  // Auto-scroll
  useEffect(() => {
    if (autoScroll && logsEndRef.current)
      logsEndRef.current.scrollIntoView({ behavior: 'smooth' })
  }, [logs, autoScroll])

  const handleScroll = () => {
    const el = scrollContainerRef.current
    if (!el) return
    setAutoScroll(el.scrollHeight - el.scrollTop - el.clientHeight < 40)
  }

  const isDone   = started && currentStep === 3
  const hasError = !!error
  const showLogs = started && !hasError && (currentStep === 1 || currentStep === 2)

  let progress  = null
  let statsRows = null
  if (stepStatus) {
    if (currentStep === 1) {
      if (stepStatus.total_queued > 0)
        progress = Math.round(((stepStatus.evaluated + stepStatus.failed) / stepStatus.total_queued) * 100)
      statsRows = [
        { label: 'Evaluated', value: stepStatus.evaluated, cls: 'green' },
        { label: 'Failed',    value: stepStatus.failed,    cls: 'red'   },
        { label: 'Remaining', value: stepStatus.remaining               },
        ...(stepStatus.total_queued > 0 ? [{ label: 'Total', value: stepStatus.total_queued }] : []),
      ]
    } else if (currentStep === 2) {
      if (stepStatus.total_queued > 0)
        progress = Math.round(((stepStatus.processed + stepStatus.failed) / stepStatus.total_queued) * 100)
      statsRows = [
        { label: 'Reviewed',  value: stepStatus.processed,       cls: 'green' },
        { label: 'Failed',    value: stepStatus.failed,          cls: 'red'   },
        { label: 'Remaining', value: stepStatus.unreviewed ?? '—'             },
        ...(stepStatus.total_queued > 0 ? [{ label: 'Total', value: stepStatus.total_queued }] : []),
      ]
    }
  }

  return (
    <div className="modal-overlay" onClick={e => e.target === e.currentTarget && onClose()}>
      <div className={`resync-container${showLogs ? ' resync-container--expanded' : ''}`}>

        {/* Header */}
        <div className="resync-header">
          <div>
            <div className="resync-title">RESYNC ALL DATA</div>
            <div className="resync-sub">
              {username}{filters.since_date ? ` · since ${filters.since_date}` : ' · all time'}
            </div>
          </div>
          <button className="btn-modal-close" onClick={onClose}>✕</button>
        </div>

        {/* Step strip */}
        <div className="resync-step-strip">
          {STEPS.map((step, i) => {
            let status
            if (!started) status = 'pending'
            else if (i < currentStep || isDone) status = 'done'
            else if (i === currentStep) status = 'active'
            else status = 'pending'
            return (
              <div key={step.key} className={`resync-strip-step resync-strip-step--${status}`}>
                <span className="resync-strip-icon">
                  {status === 'done' ? '✓' : status === 'active' ? '⟳' : '○'}
                </span>
                <span className="resync-strip-label">{step.label}</span>
                {i === 0 && refreshResult && status === 'done' && (
                  <span className="resync-strip-note">+{refreshResult.new_games ?? 0} games</span>
                )}
              </div>
            )
          })}
        </div>

        {/* Error */}
        {hasError && <div className="resync-error">{error}</div>}

        {/* Live log section */}
        {showLogs && (
          <>
            {statsRows && (
              <div className="eval-stats">
                {statsRows.map(s => (
                  <div key={s.label} className="eval-stat">
                    <span className="eval-stat-label">{s.label}</span>
                    <span className={`eval-stat-value ${s.cls || ''}`}>{s.value}</span>
                  </div>
                ))}
              </div>
            )}
            {progress !== null && (
              <div className="eval-progress-bar">
                <div className="eval-progress-fill" style={{ width: `${progress}%` }} />
                <span className="eval-progress-label">{progress}%</span>
              </div>
            )}
            <div
              className="eval-log-body resync-log-body"
              ref={scrollContainerRef}
              onScroll={handleScroll}
            >
              {logs.length === 0
                ? <div className="eval-log-empty">Waiting for logs…</div>
                : logs.map((line, i) => (
                    <div key={i} className={`eval-log-line ${line.includes('✗') ? 'error' : line.includes('✓') ? 'success' : ''}`}>
                      {line}
                    </div>
                  ))
              }
              <div ref={logsEndRef} />
            </div>
            {!autoScroll && (
              <button
                className="btn-scroll-bottom"
                onClick={() => { setAutoScroll(true); logsEndRef.current?.scrollIntoView({ behavior: 'smooth' }) }}
              >
                ↓ scroll to bottom
              </button>
            )}
          </>
        )}

        {isDone && <div className="resync-done-msg">All done!</div>}

        {/* Footer */}
        <div className="resync-footer">
          {!started ? (
            <>
              <button className="btn-ghost" onClick={onClose}>CANCEL</button>
              <button className="btn-primary" onClick={() => onStart(filters)}>START RESYNC</button>
            </>
          ) : isDone || hasError ? (
            <button className="btn-primary" onClick={onClose}>DONE</button>
          ) : (
            <>
              <span className="resync-running-note">Step {currentStep + 1} of 3 · running in background</span>
              <button className="btn-ghost" onClick={onClose}>CLOSE</button>
            </>
          )}
        </div>

      </div>
    </div>
  )
}
