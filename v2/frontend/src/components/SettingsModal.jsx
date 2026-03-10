import { useState } from 'react'
import './SettingsModal.css'

export default function SettingsModal({ currentUser, hasToken, onClose, onSave }) {
  const [username, setUsername] = useState(currentUser || '')
  const [token, setToken] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState(null)

  const handleSave = async () => {
    if (!username.trim()) return
    setSaving(true)
    setError(null)
    try {
      const body = { default_user: username.trim() }
      if (token.trim()) body.lichess_token = token.trim()
      const r = await fetch('/api/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const d = await r.json()
      if (!r.ok) throw new Error(d.detail || 'Failed to save settings')
      onSave(d.default_user, d.has_token)
    } catch (e) {
      setError(e.message)
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="modal-overlay" onClick={e => e.target === e.currentTarget && onClose()}>
      <div className="modal">
        <div className="modal-header">
          <div>
            <div className="modal-title">SETTINGS</div>
            <div className="modal-sub">Configure your default user</div>
          </div>
          <button className="modal-close" onClick={onClose}>✕</button>
        </div>

        <div className="modal-body">
          <div className="field">
            <label className="field-label">DEFAULT USERNAME</label>
            <input
              className="text-input"
              value={username}
              onChange={e => setUsername(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleSave()}
              placeholder="Lichess username"
            />
          </div>
          <div className="field">
            <label className="field-label">
              LICHESS API TOKEN{hasToken ? ' (already set — paste to replace)' : ''}
            </label>
            <input
              className="text-input"
              type="password"
              value={token}
              onChange={e => setToken(e.target.value)}
              placeholder={hasToken ? '••••••••••••••••' : 'lip_xxxxxxxxxxxx'}
              autoComplete="off"
            />
            <span className="field-hint">
              Required for bookmark sync. Generate at lichess.org/account/oauth/token — needs no special scope.
            </span>
          </div>
          {error && <div className="modal-error">{error}</div>}
        </div>

        <div className="modal-footer">
          <button className="btn-ghost" onClick={onClose}>CANCEL</button>
          <button
            className="btn-primary"
            onClick={handleSave}
            disabled={saving || !username.trim()}
          >
            {saving ? 'SAVING···' : 'SAVE'}
          </button>
        </div>
      </div>
    </div>
  )
}
