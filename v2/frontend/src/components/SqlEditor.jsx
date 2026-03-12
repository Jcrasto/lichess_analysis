import { useState, useEffect, useRef, useCallback } from 'react'
import './SqlEditor.css'

const DEFAULT_QUERY = 'SELECT * FROM games\nLIMIT 50'

export default function SqlEditor({ username, onSelectGame }) {
  const [sql, setSql] = useState(DEFAULT_QUERY)
  const [schema, setSchema] = useState({})
  const [schemaExpanded, setSchemaExpanded] = useState({})
  const [columns, setColumns] = useState([])
  const [rows, setRows] = useState([])
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(false)
  const [rowCount, setRowCount] = useState(null)
  const textareaRef = useRef(null)

  // Load schema on mount / username change
  useEffect(() => {
    if (!username) return
    fetch(`/api/sql/schema/${username}`)
      .then(r => r.json())
      .then(data => {
        setSchema(data)
        // Expand all tables by default
        const expanded = {}
        for (const t of Object.keys(data)) expanded[t] = true
        setSchemaExpanded(expanded)
      })
      .catch(() => {})
  }, [username])

  const runQuery = useCallback(async () => {
    if (!sql.trim() || !username) return
    setLoading(true)
    setError(null)
    try {
      const r = await fetch('/api/sql/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, sql }),
      })
      const d = await r.json()
      if (d.error) {
        setError(d.error)
        setColumns([])
        setRows([])
        setRowCount(null)
      } else {
        setColumns(d.columns)
        setRows(d.rows)
        setRowCount(d.rows.length)
        setError(null)
      }
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [sql, username])

  // Ctrl+Enter or Cmd+Enter to run
  const handleKeyDown = (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault()
      runQuery()
    }
  }

  const handleRowClick = async (row) => {
    if (!row.game_id) return
    try {
      const [gameRes, evalsRes] = await Promise.all([
        fetch(`/api/game/${username}/${row.game_id}`),
        fetch(`/api/evals/${username}/${row.game_id}`),
      ])
      const game = await gameRes.json()
      const evals = await evalsRes.json()
      onSelectGame(game, evals)
    } catch {}
  }

  const formatCell = (val) => {
    if (val === null || val === undefined) return <span className="sql-null">null</span>
    if (typeof val === 'boolean') return <span className="sql-bool">{String(val)}</span>
    const str = String(val)
    if (str.length > 120) return str.slice(0, 120) + '…'
    return str
  }

  return (
    <div className="sql-editor">
      {/* Schema panel */}
      <aside className="sql-schema-panel">
        <div className="sql-schema-header">SCHEMA</div>
        {Object.keys(schema).length === 0 ? (
          <div className="sql-schema-empty">No tables found</div>
        ) : (
          Object.entries(schema).map(([table, cols]) => (
            <div key={table} className="sql-table-block">
              <button
                className="sql-table-name"
                onClick={() => setSchemaExpanded(prev => ({ ...prev, [table]: !prev[table] }))}
              >
                <span className="sql-table-icon">{schemaExpanded[table] ? '▾' : '▸'}</span>
                <span className="sql-schema-name">main</span>
                <span className="sql-dot">.</span>
                <span className="sql-tname">{table}</span>
              </button>
              {schemaExpanded[table] && (
                <ul className="sql-col-list">
                  {cols.map(c => (
                    <li key={c.name} className="sql-col-item">
                      <span className="sql-col-name">{c.name}</span>
                      <span className="sql-col-type">{c.type}</span>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          ))
        )}
      </aside>

      {/* Editor + results */}
      <div className="sql-main">
        <div className="sql-editor-area">
          <textarea
            ref={textareaRef}
            className="sql-textarea"
            value={sql}
            onChange={e => setSql(e.target.value)}
            onKeyDown={handleKeyDown}
            spellCheck={false}
            placeholder="SELECT * FROM games LIMIT 50"
          />
          <div className="sql-toolbar">
            <button
              className="btn-primary sql-run-btn"
              onClick={runQuery}
              disabled={loading}
            >
              {loading ? '⏳ RUNNING…' : '▶ RUN'}
            </button>
            <span className="sql-hint">⌘↵ to run</span>
            {rowCount !== null && !error && (
              <span className="sql-row-count">{rowCount} row{rowCount !== 1 ? 's' : ''}</span>
            )}
          </div>
        </div>

        <div className="sql-results">
          {error && (
            <div className="sql-error">
              <span className="sql-error-icon">✕</span> {error}
            </div>
          )}

          {!error && columns.length > 0 && (
            <div className="sql-table-wrap">
              <table className="sql-table">
                <thead>
                  <tr>
                    {columns.map(col => (
                      <th key={col} className="sql-th">{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row, i) => (
                    <tr
                      key={i}
                      className={`sql-tr ${row.game_id ? 'sql-tr--clickable' : ''}`}
                      onClick={() => handleRowClick(row)}
                      title={row.game_id ? 'Click to view game' : undefined}
                    >
                      {columns.map(col => (
                        <td key={col} className="sql-td">{formatCell(row[col])}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {!error && columns.length === 0 && rowCount === null && !loading && (
            <div className="sql-empty">Write a query above and press Run.</div>
          )}

          {!error && rowCount === 0 && (
            <div className="sql-empty">Query returned 0 rows.</div>
          )}
        </div>
      </div>
    </div>
  )
}
