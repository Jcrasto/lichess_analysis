import { useEffect, useRef, useState, useCallback } from 'react'
import {
  ResponsiveContainer,
  BarChart, Bar,
  LineChart, Line,
  PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ReferenceLine,
} from 'recharts'
import './Dashboard.css'

const C_WIN   = '#4caf79'
const C_LOSS  = '#e05050'
const C_DRAW  = '#5a7aaf'
const C_LINE  = '#f0c040'
const C_GRID  = '#1e1e36'
const C_TEXT  = '#666'
const C_AXIS  = '#444'

const MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

function fmtMonth(ym) {
  if (!ym) return ''
  const [y, m] = ym.split('-').map(Number)
  return `${MONTH_NAMES[m - 1]} ${y}`
}

function generateMonths(minYM, maxYM) {
  if (!minYM || !maxYM) return []
  const months = []
  let [y, m] = minYM.split('-').map(Number)
  const [ey, em] = maxYM.split('-').map(Number)
  while (y < ey || (y === ey && m <= em)) {
    months.push(`${y}-${String(m).padStart(2, '0')}`)
    m++; if (m > 12) { m = 1; y++ }
  }
  return months
}

function lastDayOf(ym) {
  const [y, m] = ym.split('-').map(Number)
  return `${ym}-${new Date(y, m, 0).getDate()}`
}

function DarkTooltip({ active, payload, label, formatter }) {
  if (!active || !payload?.length) return null
  return (
    <div className="chart-tooltip">
      <div className="chart-tooltip-label">{label}</div>
      {payload.map((p, i) => (
        <div key={i} className="chart-tooltip-row" style={{ color: p.color || p.fill }}>
          <span>{p.name}:</span>
          <span>{formatter ? formatter(p.value) : p.value}</span>
        </div>
      ))}
    </div>
  )
}

function tickSampler(data, maxTicks = 12) {
  if (!data?.length) return []
  const step = Math.max(1, Math.ceil(data.length / maxTicks))
  return data.filter((_, i) => i % step === 0).map(d => d.period)
}

export default function Dashboard({ username, appliedFilters = {}, onPendingChange, onPendingDatesChange }) {
  const [analytics, setAnalytics]   = useState(null)
  const [loading, setLoading]        = useState(false)
  const [months, setMonths]          = useState([])
  const [lowIdx, setLowIdx]          = useState(0)
  const [highIdx, setHighIdx]        = useState(0)
  const [activePreset, setActivePreset] = useState('All Time')
  const initialized                  = useRef(false)
  const debounceRef                  = useRef(null)
  const sliderRef                    = useRef(null)
  const appliedFiltersRef            = useRef(appliedFilters)

  // Keep ref in sync with prop
  useEffect(() => {
    appliedFiltersRef.current = appliedFilters
  }, [appliedFilters])

  const PRESETS = [
    { label: 'All Time',      months: null },
    { label: 'Last Year',     months: 12   },
    { label: 'Last 6 Months', months: 6    },
    { label: 'Last 90 Days',  months: 3    },
    { label: 'Last 30 Days',  months: 1    },
  ]

  const fetchAnalytics = useCallback(async (sinceYM, untilYM) => {
    if (!username) return
    setLoading(true)
    try {
      const af = appliedFiltersRef.current
      const params = new URLSearchParams()
      if (sinceYM) params.set('since_date', sinceYM + '-01')
      if (untilYM) params.set('until_date', lastDayOf(untilYM))
      if (af.color) params.set('color', af.color)
      if (af.outcome) params.set('outcome', af.outcome)
      if (af.perf_type) params.set('perf_type', af.perf_type)
      if (af.opening) params.set('opening', af.opening)
      if (af.termination) params.set('termination', af.termination)
      if (af.min_moves) params.set('min_moves', af.min_moves)
      if (af.max_moves) params.set('max_moves', af.max_moves)
      const r = await fetch(`/api/analytics/${username}?${params}`)
      const data = await r.json()
      setAnalytics(data)
      return data
    } catch (e) {
      console.error('Analytics fetch failed', e)
    } finally {
      setLoading(false)
    }
  }, [username])

  // Initialize on username change
  useEffect(() => {
    initialized.current = false
    setAnalytics(null)
    setMonths([])
    setLowIdx(0)
    setHighIdx(0)

    // Fetch just to get date_range for slider bounds
    fetch(`/api/analytics/${username}`)
      .then(r => r.json())
      .then(data => {
        if (!data?.date_range?.min || !data?.date_range?.max) return
        const ms = generateMonths(
          data.date_range.min.slice(0, 7),
          data.date_range.max.slice(0, 7)
        )
        setMonths(ms)

        // Determine initial slider based on appliedFilters
        const currentAf = appliedFiltersRef.current
        let initialLow = 0
        let initialHigh = ms.length - 1

        if (currentAf.since_date) {
          const sinceYM = currentAf.since_date.slice(0, 7)
          const idx = ms.findIndex(m => m >= sinceYM)
          if (idx >= 0) initialLow = idx
        }
        if (currentAf.until_date) {
          const untilYM = currentAf.until_date.slice(0, 7)
          for (let i = ms.length - 1; i >= 0; i--) {
            if (ms[i] <= untilYM) { initialHigh = i; break }
          }
        }

        setLowIdx(initialLow)
        setHighIdx(initialHigh)

        if (!currentAf.since_date && !currentAf.until_date) {
          setActivePreset('All Time')
        } else {
          setActivePreset(null)
        }

        initialized.current = true
        fetchAnalytics(
          initialLow === 0 && !currentAf.since_date ? null : ms[initialLow],
          initialHigh === ms.length - 1 && !currentAf.until_date ? null : ms[initialHigh]
        )
      })
  }, [username, fetchAnalytics])

  // Refetch analytics when appliedFilters changes (after Apply button)
  useEffect(() => {
    if (!initialized.current || !months.length) return

    const af = appliedFilters
    let newLow = lowIdx
    let newHigh = highIdx

    // Sync slider to applied date range
    if (af.since_date) {
      const sinceYM = af.since_date.slice(0, 7)
      const idx = months.findIndex(m => m >= sinceYM)
      if (idx >= 0) newLow = idx
    } else {
      newLow = 0
    }
    if (af.until_date) {
      const untilYM = af.until_date.slice(0, 7)
      for (let i = months.length - 1; i >= 0; i--) {
        if (months[i] <= untilYM) { newHigh = i; break }
      }
    } else {
      newHigh = months.length - 1
    }

    if (!af.since_date && !af.until_date) {
      setActivePreset('All Time')
    } else {
      setActivePreset(null)
    }

    setLowIdx(newLow)
    setHighIdx(newHigh)

    fetchAnalytics(
      newLow === 0 && !af.since_date ? null : months[newLow],
      newHigh === months.length - 1 && !af.until_date ? null : months[newHigh]
    )
  }, [appliedFilters]) // eslint-disable-line react-hooks/exhaustive-deps

  const handleSlider = (newLow, newHigh) => {
    setActivePreset(null)
    setLowIdx(newLow)
    setHighIdx(newHigh)
    clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => {
      fetchAnalytics(months[newLow], months[newHigh])
      onPendingDatesChange?.(months[newLow] + '-01', lastDayOf(months[newHigh]))
    }, 400)
  }

  const applyPreset = useCallback((label, numMonths) => {
    if (!months.length) return
    setActivePreset(label)
    const newHigh = months.length - 1
    let newLow = 0
    if (numMonths !== null) {
      const today = new Date()
      const target = new Date(today.getFullYear(), today.getMonth() - numMonths, 1)
      const targetYM = `${target.getFullYear()}-${String(target.getMonth() + 1).padStart(2, '0')}`
      newLow = months.findIndex(m => m >= targetYM)
      if (newLow < 0) newLow = 0
    }
    setLowIdx(newLow)
    setHighIdx(newHigh)
    clearTimeout(debounceRef.current)
    fetchAnalytics(numMonths === null ? null : months[newLow], months[newHigh])
    if (numMonths === null) {
      onPendingDatesChange?.('', '')
    } else {
      onPendingDatesChange?.(months[newLow] + '-01', lastDayOf(months[newHigh]))
    }
  }, [months, fetchAnalytics, onPendingDatesChange])

  // Chart click handlers
  const handlePieClick = (entry) => {
    const map = { 'Wins': 'win', 'Losses': 'loss', 'Draws': 'draw' }
    const outcome = map[entry.name]
    if (outcome && onPendingChange) onPendingChange({ outcome })
  }

  const handleTimeControlClick = (data) => {
    if (data?.perf_type && onPendingChange) onPendingChange({ perf_type: data.perf_type })
  }

  const handleTerminationClick = (data) => {
    if (data?.term && onPendingChange) onPendingChange({ termination: data.term })
  }

  const handleOpeningClick = (opening, sinceYM, untilYM) => {
    if (!onPendingChange) return
    const sinceDate = sinceYM ? sinceYM + '-01' : ''
    const untilDate = untilYM ? lastDayOf(untilYM) : ''
    onPendingChange({ opening, since_date: sinceDate, until_date: untilDate })
  }

  // ── derived stats
  const summary   = analytics?.result_summary || {}
  const total     = summary.total || 0
  const winRate   = total > 0 ? ((summary.wins / total) * 100).toFixed(1) : '—'
  const peakElo   = analytics?.elo_per_month?.length
    ? Math.max(...analytics.elo_per_month.map(d => d.elo))
    : null
  const avgElo    = analytics?.elo_per_month?.length
    ? Math.round(analytics.elo_per_month.reduce((s, d) => s + d.elo, 0) / analytics.elo_per_month.length)
    : null
  const topOpening = analytics?.top_openings?.[0]?.opening ?? '—'

  const sliderMax = Math.max(0, months.length - 1)
  const lowPct    = sliderMax > 0 ? (lowIdx / sliderMax) * 100 : 0
  const highPct   = sliderMax > 0 ? (highIdx / sliderMax) * 100 : 100

  const xTicksMonthly = tickSampler(analytics?.games_per_month)
  const xTicksElo     = tickSampler(analytics?.elo_per_month)

  const pieData = [
    { name: 'Wins',   value: summary.wins   || 0, color: C_WIN  },
    { name: 'Losses', value: summary.losses || 0, color: C_LOSS },
    { name: 'Draws',  value: summary.draws  || 0, color: C_DRAW },
  ]

  return (
    <div className="dashboard">

      {/* ── STAT CARDS */}
      <div className="dash-stats">
        <StatCard label="Total Games"   value={total.toLocaleString()} />
        <StatCard label="Win Rate"      value={total > 0 ? `${winRate}%` : '—'} color={C_WIN} />
        <StatCard label="Losses"        value={total > 0 ? `${((summary.losses/total)*100).toFixed(1)}%` : '—'} color={C_LOSS} />
        <StatCard label="Draws"         value={total > 0 ? `${((summary.draws/total)*100).toFixed(1)}%` : '—'} color={C_DRAW} />
        <StatCard label="Avg ELO"       value={avgElo ?? '—'} />
        <StatCard label="Peak ELO"      value={peakElo ?? '—'} color={C_LINE} />
        <StatCard label="Top Opening"   value={topOpening} small />
      </div>

      {/* ── DATE RANGE SLIDER */}
      {months.length > 1 && (
        <div className="dash-slider-section">
          <div className="dash-slider-header">
            <span className="dash-slider-label">DATE RANGE</span>
            <span className="dash-slider-range">
              {fmtMonth(months[lowIdx])} → {fmtMonth(months[highIdx])}
              {loading && <span className="dash-loading"> ···</span>}
            </span>
          </div>

          <div className="dash-presets">
            {PRESETS.map(p => (
              <button
                key={p.label}
                className={`dash-preset-btn ${activePreset === p.label ? 'active' : ''}`}
                onClick={() => applyPreset(p.label, p.months)}
              >
                {p.label}
              </button>
            ))}
          </div>
          <div className="dual-slider" ref={sliderRef}
            onClick={(e) => {
              if (e.target.type === 'range') return
              const rect = e.currentTarget.getBoundingClientRect()
              const pct = (e.clientX - rect.left) / rect.width
              const clickIdx = Math.round(pct * sliderMax)
              const distToLow = Math.abs(clickIdx - lowIdx)
              const distToHigh = Math.abs(clickIdx - highIdx)
              if (distToLow <= distToHigh) {
                handleSlider(Math.min(clickIdx, highIdx - 1), highIdx)
              } else {
                handleSlider(lowIdx, Math.max(clickIdx, lowIdx + 1))
              }
            }}
          >
            <div className="dual-slider-track">
              <div
                className="dual-slider-fill"
                style={{ left: `${lowPct}%`, right: `${100 - highPct}%` }}
              />
            </div>
            <input
              type="range" min={0} max={sliderMax}
              value={lowIdx}
              onChange={e => {
                const v = Math.min(Number(e.target.value), highIdx - 1)
                handleSlider(v, highIdx)
              }}
              className="dual-slider-thumb"
            />
            <input
              type="range" min={0} max={sliderMax}
              value={highIdx}
              onChange={e => {
                const v = Math.max(Number(e.target.value), lowIdx + 1)
                handleSlider(lowIdx, v)
              }}
              className="dual-slider-thumb"
            />
          </div>
        </div>
      )}

      {!analytics && loading && (
        <div className="dash-loading-state">Loading analytics···</div>
      )}

      {analytics && (
        <>
          {/* ── GAMES PER MONTH */}
          <div className="dash-chart-card">
            <div className="dash-chart-title">GAMES PER MONTH</div>
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={analytics.games_per_month} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
                <CartesianGrid stroke={C_GRID} vertical={false} />
                <XAxis dataKey="period" ticks={xTicksMonthly} tick={{ fill: C_TEXT, fontSize: 11 }} axisLine={{ stroke: C_AXIS }} tickLine={false} />
                <YAxis tick={{ fill: C_TEXT, fontSize: 11 }} axisLine={false} tickLine={false} width={32} />
                <Tooltip content={<DarkTooltip />} />
                <Legend iconSize={10} wrapperStyle={{ fontSize: 11, color: '#888', paddingTop: 8 }} />
                <Bar dataKey="wins"   name="Wins"   stackId="a" fill={C_WIN}  radius={[0,0,0,0]} maxBarSize={24} />
                <Bar dataKey="draws"  name="Draws"  stackId="a" fill={C_DRAW} maxBarSize={24} />
                <Bar dataKey="losses" name="Losses" stackId="a" fill={C_LOSS} radius={[3,3,0,0]} maxBarSize={24} />
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* ── ELO PROGRESSION */}
          {analytics.elo_per_month.length > 0 && (
            <div className="dash-chart-card">
              <div className="dash-chart-title">ELO PROGRESSION</div>
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={analytics.elo_per_month} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
                  <CartesianGrid stroke={C_GRID} vertical={false} />
                  <XAxis dataKey="period" ticks={xTicksElo} tick={{ fill: C_TEXT, fontSize: 11 }} axisLine={{ stroke: C_AXIS }} tickLine={false} />
                  <YAxis tick={{ fill: C_TEXT, fontSize: 11 }} axisLine={false} tickLine={false} width={44} domain={['auto', 'auto']} />
                  {avgElo && <ReferenceLine y={avgElo} stroke="#333" strokeDasharray="4 3" label={{ value: `avg ${avgElo}`, fill: '#555', fontSize: 10, position: 'insideTopRight' }} />}
                  <Tooltip content={<DarkTooltip formatter={v => v} />} />
                  <Line type="monotone" dataKey="elo" name="ELO" stroke={C_LINE} strokeWidth={2} dot={false} activeDot={{ r: 4, fill: C_LINE }} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* ── TOP OPENINGS */}
          {analytics.top_openings.length > 0 && (
            <div className="dash-chart-card">
              <div className="dash-chart-title">
                TOP OPENINGS
                {onPendingChange && (
                  <span className="dash-opening-hint">click to filter games</span>
                )}
              </div>
              <div className="opening-list">
                {analytics.top_openings.map(row => (
                  <OpeningRow
                    key={row.opening}
                    row={row}
                    onClick={onPendingChange ? () => handleOpeningClick(row.opening, months[lowIdx], months[highIdx]) : null}
                  />
                ))}
              </div>
            </div>
          )}

          {/* ── BOTTOM ROW: pie + time control */}
          <div className="dash-bottom-row">
            {/* Result distribution - clickable */}
            <div className="dash-chart-card dash-chart-card--half">
              <div className="dash-chart-title">
                RESULT DISTRIBUTION
                {onPendingChange && <span className="dash-opening-hint">click to filter</span>}
              </div>
              <div className="dash-pie-wrap">
                <PieChart width={200} height={200}>
                  <Pie
                    data={pieData} cx="50%" cy="50%"
                    innerRadius={55} outerRadius={85}
                    paddingAngle={2} dataKey="value"
                  >
                    {pieData.map((entry, i) => (
                      <Cell
                        key={i}
                        fill={entry.color}
                        stroke="none"
                        style={{ cursor: onPendingChange ? 'pointer' : 'default' }}
                        onClick={() => onPendingChange && handlePieClick(entry)}
                      />
                    ))}
                  </Pie>
                  <Tooltip
                    formatter={(v, n) => [`${v} (${total > 0 ? ((v/total)*100).toFixed(1) : 0}%)`, n]}
                    contentStyle={{ background: '#12121e', border: '1px solid #2a2a4a', borderRadius: 6, fontSize: 12 }}
                    itemStyle={{ color: '#ccc' }}
                  />
                </PieChart>
                <div className="dash-pie-legend">
                  {pieData.map(d => (
                    <div
                      key={d.name}
                      className={`dash-pie-row ${onPendingChange ? 'dash-pie-row--clickable' : ''}`}
                      onClick={() => onPendingChange && handlePieClick(d)}
                    >
                      <span className="dash-pie-dot" style={{ background: d.color }} />
                      <span className="dash-pie-name">{d.name}</span>
                      <span className="dash-pie-val">{d.value}</span>
                      <span className="dash-pie-pct">
                        {total > 0 ? `${((d.value / total) * 100).toFixed(1)}%` : '—'}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            {/* Time control breakdown - clickable */}
            {analytics.perf_breakdown.length > 0 && (
              <div className="dash-chart-card dash-chart-card--half">
                <div className="dash-chart-title">
                  TIME CONTROLS
                  {onPendingChange && <span className="dash-opening-hint">click to filter</span>}
                </div>
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart
                    data={analytics.perf_breakdown}
                    margin={{ top: 4, right: 16, bottom: 0, left: 0 }}
                    style={{ cursor: onPendingChange ? 'pointer' : 'default' }}
                  >
                    <CartesianGrid stroke={C_GRID} vertical={false} />
                    <XAxis dataKey="perf_type" tick={{ fill: C_TEXT, fontSize: 11 }} axisLine={{ stroke: C_AXIS }} tickLine={false} />
                    <YAxis tick={{ fill: C_TEXT, fontSize: 11 }} axisLine={false} tickLine={false} width={36} />
                    <Tooltip content={<DarkTooltip />} />
                    <Bar dataKey="wins"   name="Wins"   stackId="a" fill={C_WIN}  maxBarSize={36} onClick={handleTimeControlClick} />
                    <Bar dataKey="draws"  name="Draws"  stackId="a" fill={C_DRAW} maxBarSize={36} onClick={handleTimeControlClick} />
                    <Bar dataKey="losses" name="Losses" stackId="a" fill={C_LOSS} radius={[3,3,0,0]} maxBarSize={36} onClick={handleTimeControlClick} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}
          </div>

          {/* ── TERMINATION BREAKDOWN */}
          {analytics.termination_breakdown?.length > 0 && (
            <div className="dash-chart-card">
              <div className="dash-chart-title">
                GAME ENDINGS
                {onPendingChange && <span className="dash-opening-hint">click to filter</span>}
              </div>
              <ResponsiveContainer width="100%" height={Math.max(160, analytics.termination_breakdown.length * 50)}>
                <BarChart
                  data={analytics.termination_breakdown}
                  layout="vertical"
                  margin={{ top: 4, right: 16, bottom: 0, left: 90 }}
                  style={{ cursor: onPendingChange ? 'pointer' : 'default' }}
                >
                  <CartesianGrid stroke={C_GRID} horizontal={false} />
                  <XAxis type="number" tick={{ fill: C_TEXT, fontSize: 11 }} axisLine={false} tickLine={false} />
                  <YAxis type="category" dataKey="term" tick={{ fill: '#aaa', fontSize: 11 }} axisLine={false} tickLine={false} width={90} />
                  <Tooltip content={<DarkTooltip />} />
                  <Bar dataKey="wins"   name="Wins"   stackId="a" fill={C_WIN}  maxBarSize={28} onClick={handleTerminationClick} />
                  <Bar dataKey="draws"  name="Draws"  stackId="a" fill={C_DRAW} maxBarSize={28} onClick={handleTerminationClick} />
                  <Bar dataKey="losses" name="Losses" stackId="a" fill={C_LOSS} radius={[0,3,3,0]} maxBarSize={28} onClick={handleTerminationClick} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* ── MOVE COUNT DISTRIBUTION */}
          {analytics.moves_distribution?.length > 0 && (
            <div className="dash-chart-card">
              <div className="dash-chart-title">MOVE COUNT DISTRIBUTION</div>
              <ResponsiveContainer width="100%" height={200}>
                <BarChart data={analytics.moves_distribution} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
                  <CartesianGrid stroke={C_GRID} vertical={false} />
                  <XAxis dataKey="range" tick={{ fill: C_TEXT, fontSize: 11 }} axisLine={{ stroke: C_AXIS }} tickLine={false} />
                  <YAxis tick={{ fill: C_TEXT, fontSize: 11 }} axisLine={false} tickLine={false} width={36} />
                  <Tooltip content={<DarkTooltip />} />
                  <Bar dataKey="wins"   name="Wins"   stackId="a" fill={C_WIN}  maxBarSize={28} />
                  <Bar dataKey="draws"  name="Draws"  stackId="a" fill={C_DRAW} maxBarSize={28} />
                  <Bar dataKey="losses" name="Losses" stackId="a" fill={C_LOSS} radius={[3,3,0,0]} maxBarSize={28} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}
        </>
      )}
    </div>
  )
}

function OpeningRow({ row, onClick }) {
  const total = row.wins + row.draws + row.losses
  const winPct  = total > 0 ? ((row.wins   / total) * 100).toFixed(1) : '0.0'
  const drawPct = total > 0 ? ((row.draws  / total) * 100).toFixed(1) : '0.0'
  const lossPct = total > 0 ? ((row.losses / total) * 100).toFixed(1) : '0.0'

  return (
    <div
      className={`opening-row ${onClick ? 'opening-row--clickable' : ''}`}
      onClick={onClick}
      title={onClick ? `Filter games: ${row.opening}` : undefined}
    >
      <div className="opening-row-top">
        <span className="opening-row-name">{row.opening}</span>
        <span className="opening-row-total">{total.toLocaleString()} games</span>
      </div>
      <div className="opening-row-bar">
        {row.wins   > 0 && <div className="orb-win"  style={{ flex: row.wins   }} />}
        {row.draws  > 0 && <div className="orb-draw" style={{ flex: row.draws  }} />}
        {row.losses > 0 && <div className="orb-loss" style={{ flex: row.losses }} />}
      </div>
      <div className="opening-row-stats">
        <span className="ors-win">  {row.wins.toLocaleString()}  <em>({winPct}%)</em></span>
        <span className="ors-draw"> {row.draws.toLocaleString()} <em>({drawPct}%)</em></span>
        <span className="ors-loss"> {row.losses.toLocaleString()}<em>({lossPct}%)</em></span>
      </div>
    </div>
  )
}

function StatCard({ label, value, color, small }) {
  return (
    <div className="stat-card">
      <div className="stat-card-label">{label}</div>
      <div className={`stat-card-value ${small ? 'stat-card-value--small' : ''}`} style={color ? { color } : {}}>
        {value}
      </div>
    </div>
  )
}
