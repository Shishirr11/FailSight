import { useState, useEffect, useCallback } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  LineChart, Line, CartesianGrid
} from 'recharts'
import {
  AlertTriangle, TrendingDown, DollarSign, Search,
  X, ExternalLink, FlaskConical
} from 'lucide-react'
import { fetchFailureStats, fetchFailures, fetchFailure } from '../api/failures'


const fmt = n => !n || n === 0 ? '$0'
  : n >= 1e9 ? `$${(n/1e9).toFixed(1)}B`
  : n >= 1e6 ? `$${(n/1e6).toFixed(1)}M`
  : n >= 1e3 ? `$${(n/1e3).toFixed(0)}K`
  : `$${n}`

const REASON_LABELS = {
  competition_giants:   'Tech Giants',
  cash:                 'Ran Out of Cash',
  competition:          'Competition',
  no_pmf:               'No Product-Market Fit',
  acquisition:          'Acquisition Stagnation',
  platform_dependency:  'Platform Dependency',
  monetization:         'Monetization Failure',
  niche:                'Too Niche',
  execution:            'Execution Flaws',
  trend_shift:          'Trend Shifts',
  trust:                'Trust Issues',
  regulatory:           'Regulatory Pressure',
  overhype:             'Overhype',
  high_costs:           'High Operational Costs',
  team:                 'Team / Management',
  timing:               'Bad Timing',
  focus:                'Lack of Focus',
  pivot:                'Failure to Pivot',
  unknown:              'Unknown',
}

const CHART_TOOLTIP_STYLE = {
  contentStyle: { background: '#1e293b', border: '1px solid rgba(255,255,255,0.08)', borderRadius: 8, fontSize: 12 },
  labelStyle:   { color: '#94a3b8' },
  itemStyle:    { color: '#e2e8f0' },
}


function StatCard({ icon: Icon, label, value, sub, color = 'text-brand-400' }) {
  return (
    <div className="card p-4">
      <div className="flex items-center gap-2 mb-2">
        <Icon size={13} className={color} />
        <span className="section-label">{label}</span>
      </div>
      <p className="stat-num">{value}</p>
      {sub && <p className="text-2xs text-slate-500 mt-1">{sub}</p>}
    </div>
  )
}

function ReasonBadge({ reason }) {
  return (
    <span className="text-2xs px-2 py-0.5 rounded-full bg-danger-500/10
                     text-danger-400 border border-danger-500/20 font-medium">
      {REASON_LABELS[reason] || reason}
    </span>
  )
}


function FailureDetail({ failureId, onClose }) {
  const [detail, setDetail]   = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetchFailure(failureId)
      .then(setDetail).catch(console.error)
      .finally(() => setLoading(false))
  }, [failureId])

  return (
    <div className="fixed inset-0 z-50 flex justify-end">
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={onClose} />
      <div className="relative w-full max-w-lg bg-surface-900 border-l border-white/[0.08]
                      h-full overflow-y-auto shadow-2xl animate-slide-left">
        {loading ? (
          <div className="flex items-center justify-center h-full text-slate-500 text-sm">
            Loading…
          </div>
        ) : detail ? (
          <div className="p-6">
            {}
            <div className="flex justify-between items-start mb-5">
              <div className="flex-1 min-w-0">
                <h2 className="text-lg font-bold text-slate-100">{detail.company_name}</h2>
                {detail.description && (
                  <p className="text-sm text-slate-500 mt-1 line-clamp-2">{detail.description}</p>
                )}
              </div>
              <button onClick={onClose}
                className="btn-ghost p-2 ml-3 shrink-0 text-slate-400 hover:text-slate-200">
                <X size={16} />
              </button>
            </div>

            {}
            <div className="grid grid-cols-2 gap-2 mb-5">
              {[
                ['Sector',    detail.sector],
                ['Country',   detail.country],
                ['Founded',   detail.year_founded],
                ['Closed',    detail.year_failed],
                ['Outcome',   detail.outcome],
                ['Funding',   detail.funding_raised_usd ? fmt(detail.funding_raised_usd) : detail.funding_range],
                ['Founders',  detail.founder_names],
                ['Investors', detail.num_investors ? `${detail.num_investors} investors` : null],
                ['Rounds',    detail.num_funding_rounds ? `${detail.num_funding_rounds} rounds` : null],
              ].filter(([, v]) => v).map(([label, val]) => (
                <div key={label} className="bg-white/[0.03] rounded-lg p-3">
                  <p className="text-2xs text-slate-500 mb-0.5">{label}</p>
                  <p className="text-xs font-semibold text-slate-200">{val}</p>
                </div>
              ))}
            </div>

            {}
            {detail.failure_reasons?.length > 0 && (
              <div className="mb-5">
                <p className="section-label mb-2">Why It Failed</p>
                <div className="flex flex-wrap gap-1.5">
                  {detail.failure_reasons.map(r => <ReasonBadge key={r} reason={r} />)}
                </div>
              </div>
            )}

            {}
            {detail.why_failed && (
              <div className="mb-4 p-3 rounded-xl bg-danger-500/5 border border-danger-500/15">
                <p className="text-2xs font-semibold text-danger-400 mb-1.5 uppercase tracking-wide">Cause</p>
                <p className="text-xs text-slate-300 leading-relaxed">{detail.why_failed}</p>
              </div>
            )}

            {}
            {detail.key_lesson && (
              <div className="mb-4 p-3 rounded-xl bg-warning-500/5 border border-warning-500/15">
                <p className="text-2xs font-semibold text-warning-400 mb-1.5 uppercase tracking-wide">Key Lesson</p>
                <p className="text-xs text-slate-300 leading-relaxed">{detail.key_lesson}</p>
              </div>
            )}

            {}
            {detail.full_article && (
              <div className="mb-5">
                <p className="section-label mb-2">Full Post-Mortem</p>
                <div className="text-xs text-slate-400 leading-relaxed space-y-3
                                max-h-56 overflow-y-auto pr-1">
                  {detail.full_article.split('\n\n').filter(Boolean).map((para, i) => (
                    <p key={i}>{para}</p>
                  ))}
                </div>
              </div>
            )}

            {detail.source_url && (
              <a href={detail.source_url} target="_blank" rel="noreferrer"
                className="flex items-center gap-1.5 text-xs text-brand-400 hover:underline">
                <ExternalLink size={12} /> Read full analysis
              </a>
            )}
          </div>
        ) : (
          <div className="flex items-center justify-center h-full text-slate-500 text-sm">
            Not found.
          </div>
        )}
      </div>
    </div>
  )
}


const ALL_REASONS = Object.keys(REASON_LABELS)

export default function TrialAndErrors() {
  const [stats,         setStats]         = useState(null)
  const [results,       setResults]       = useState([])
  const [total,         setTotal]         = useState(0)
  const [offset,        setOffset]        = useState(0)
  const [loading,       setLoading]       = useState(false)
  const [statsLoading,  setStatsLoading]  = useState(true)
  const [error,         setError]         = useState(null)
  const [selectedId,    setSelectedId]    = useState(null)

  const [q,       setQ]       = useState('')
  const [sector,  setSector]  = useState('')
  const [reason,  setReason]  = useState('')
  const [minYear, setMinYear] = useState('')
  const [source,  setSource]  = useState('')

  useEffect(() => {
    fetchFailureStats()
      .then(setStats).catch(e => setError(e.message))
      .finally(() => setStatsLoading(false))
  }, [])

  const load = useCallback(async (off = 0) => {
    setLoading(true); setError(null)
    try {
      const data = await fetchFailures({
        q, sector, reason, source,
        minYear: minYear || null, maxYear: null,
        limit: 20, offset: off,
      })
      setResults(data.results); setTotal(data.total); setOffset(off)
    } catch (e) { setError(e.message) }
    finally { setLoading(false) }
  }, [q, sector, reason, minYear, source])

  useEffect(() => { load(0) }, []) 

  return (
    <div className="p-5 max-w-6xl mx-auto space-y-4">

      {}
      <div className="flex items-center gap-3">
        <div className="w-9 h-9 rounded-xl bg-danger-500/10 border border-danger-500/20
                        flex items-center justify-center shrink-0">
          <FlaskConical size={16} className="text-danger-400" />
        </div>
        <div>
          <h1 className="text-lg font-bold text-slate-100">Trial & Errors</h1>
          <p className="text-xs text-slate-500">
            {stats ? `${stats.total.toLocaleString()} startup failures analyzed` : '…'} —
            learn what killed companies before you build in their space
          </p>
        </div>
      </div>

      {}
      {!statsLoading && stats && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <StatCard icon={AlertTriangle} label="Total Failures"
            value={stats.total.toLocaleString()} color="text-danger-400" />
          <StatCard icon={DollarSign} label="Avg Burned"
            value={fmt(stats.avg_funding_burned)} sub="per failed startup" color="text-warning-400" />
          <StatCard icon={TrendingDown} label="Total Capital Lost"
            value={fmt(stats.total_funding_burned)}
            sub={`${stats.failures_with_funding} funded failures`} color="text-danger-400" />
          <StatCard icon={AlertTriangle} label="Top Reason"
            value={REASON_LABELS[stats.top_reasons?.[0]?.reason] || '—'}
            sub={`${stats.top_reasons?.[0]?.count} companies`} color="text-purple-400" />
        </div>
      )}

      {}
      {stats && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="card p-4">
            <p className="text-sm font-semibold text-slate-200 mb-4">Top Failure Reasons</p>
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={stats.top_reasons.slice(0, 10)} layout="vertical"
                margin={{ left: 130, right: 16, top: 0, bottom: 0 }}>
                <XAxis type="number" tick={{ fontSize: 11, fill: '#64748b' }} axisLine={false} tickLine={false} />
                <YAxis type="category" dataKey="reason" width={125}
                  tick={{ fontSize: 11, fill: '#94a3b8' }}
                  tickFormatter={r => REASON_LABELS[r] || r}
                  axisLine={false} tickLine={false} />
                <Tooltip {...CHART_TOOLTIP_STYLE}
                  formatter={(v, n) => [v, REASON_LABELS[n] || n]} />
                <Bar dataKey="count" fill="#ef4444" radius={[0, 4, 4, 0]} opacity={0.85} />
              </BarChart>
            </ResponsiveContainer>
          </div>

          <div className="card p-4">
            <p className="text-sm font-semibold text-slate-200 mb-4">Failures by Year</p>
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={stats.by_year}
                margin={{ left: 0, right: 16, top: 4, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
                <XAxis dataKey="year" tick={{ fontSize: 11, fill: '#64748b' }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fontSize: 11, fill: '#64748b' }} axisLine={false} tickLine={false} />
                <Tooltip {...CHART_TOOLTIP_STYLE} />
                <Line type="monotone" dataKey="count" stroke="#ef4444"
                  strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {}
      {stats?.by_sector?.length > 0 && (
        <div className="card p-4">
          <p className="section-label mb-3">Failures by Sector</p>
          <div className="flex flex-wrap gap-2">
            {stats.by_sector.map(s => (
              <button key={s.sector}
                onClick={() => { setSector(s.sector); load(0) }}
                className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full border
                            text-xs font-medium transition-colors
                            ${sector === s.sector
                              ? 'bg-danger-500/10 border-danger-500/30 text-danger-400'
                              : 'border-white/[0.08] text-slate-400 hover:border-danger-500/30 hover:text-danger-400'
                            }`}>
                {s.sector}
                <span className="bg-danger-500/15 text-danger-400 rounded-full px-1.5 py-0.5 text-2xs font-bold">
                  {s.count}
                </span>
              </button>
            ))}
          </div>
        </div>
      )}

      {}
      <div className="card p-4 space-y-3">
        <div className="grid grid-cols-1 md:grid-cols-5 gap-3">
          <div className="relative md:col-span-2">
            <Search size={13} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500" />
            <input className="input pl-8" type="text" value={q}
              onChange={e => setQ(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && load(0)}
              placeholder="Search company or lesson…" />
          </div>
          <input className="input" type="text" value={sector}
            onChange={e => setSector(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && load(0)}
            placeholder="Sector…" />
          <select className="input bg-surface-800" value={reason}
            onChange={e => setReason(e.target.value)}>
            <option value="">All reasons</option>
            {ALL_REASONS.filter(r => r !== 'unknown').map(r => (
              <option key={r} value={r}>{REASON_LABELS[r]}</option>
            ))}
          </select>
          <div className="flex gap-2">
            <input className="input w-20" type="number" value={minYear}
              onChange={e => setMinYear(e.target.value)}
              placeholder="Year" min="1990" max="2026" />
            <select className="input flex-1 bg-surface-800" value={source}
              onChange={e => setSource(e.target.value)}>
              <option value="">All sources</option>
              <option value="cbinsights">CB Insights</option>
              <option value="failory">Failory</option>
            </select>
            <button onClick={() => load(0)} className="btn-primary px-3 whitespace-nowrap">
              Go
            </button>
          </div>
        </div>

        {}
        {(sector || reason || q) && (
          <div className="flex flex-wrap gap-2">
            {[
              [sector, () => { setSector(''); load(0) }],
              [reason && REASON_LABELS[reason], () => { setReason(''); load(0) }],
              [q && `"${q}"`, () => { setQ(''); load(0) }],
            ].filter(([v]) => v).map(([label, clear], i) => (
              <button key={i} onClick={clear}
                className="flex items-center gap-1 text-xs px-2.5 py-1 rounded-full
                           bg-danger-500/10 text-danger-400 border border-danger-500/20">
                {label} <X size={10} />
              </button>
            ))}
          </div>
        )}
      </div>

      {}
      {error && (
        <div className="rounded-xl bg-danger-500/10 border border-danger-500/20
                        text-danger-400 px-4 py-3 text-sm">
          {error}
        </div>
      )}

      {}
      {loading ? (
        <div className="text-center py-16 text-slate-500 text-sm">Loading failures…</div>
      ) : results.length === 0 ? (
        <div className="text-center py-16 text-slate-500 text-sm">No failures found.</div>
      ) : (
        <>
          <p className="text-2xs text-slate-600">
            Showing {offset + 1}–{Math.min(offset + results.length, total)} of {total.toLocaleString()}
          </p>
          <div className="space-y-2">
            {results.map(f => (
              <button key={f.failure_id} onClick={() => setSelectedId(f.failure_id)}
                className="card-hover w-full text-left p-4">
                <div className="flex items-start justify-between gap-3">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1.5 flex-wrap">
                      <span className="font-semibold text-sm text-slate-100">{f.company_name}</span>
                      {f.sector && (
                        <span className="text-2xs px-2 py-0.5 rounded-full bg-white/[0.05]
                                         text-slate-400 border border-white/[0.06]">
                          {f.sector}
                        </span>
                      )}
                      {f.outcome && (
                        <span className={`text-2xs px-2 py-0.5 rounded-full font-medium ${
                          f.outcome === 'Bankruptcy'
                            ? 'bg-danger-500/10 text-danger-400 border border-danger-500/20'
                            : f.outcome === 'Acquired'
                            ? 'bg-brand-500/10 text-brand-400 border border-brand-500/20'
                            : 'bg-white/[0.05] text-slate-400 border border-white/[0.06]'
                        }`}>{f.outcome}</span>
                      )}
                    </div>
                    {f.description && (
                      <p className="text-xs text-slate-500 mb-2 line-clamp-1">{f.description}</p>
                    )}
                    <div className="flex flex-wrap gap-1.5">
                      {(f.failure_reasons || []).slice(0, 4).map(r => (
                        <ReasonBadge key={r} reason={r} />
                      ))}
                    </div>
                    {f.key_lesson && (
                      <p className="text-xs text-warning-400/80 mt-2 italic line-clamp-1">
                        💡 {f.key_lesson}
                      </p>
                    )}
                  </div>
                  <div className="text-right shrink-0">
                    {f.funding_raised_usd > 0 && (
                      <p className="text-sm font-bold text-danger-400">{fmt(f.funding_raised_usd)}</p>
                    )}
                    {f.year_failed && (
                      <p className="text-2xs text-slate-500 mt-0.5">
                        {f.year_founded && `${f.year_founded}–`}{f.year_failed}
                      </p>
                    )}
                  </div>
                </div>
              </button>
            ))}
          </div>

          <div className="flex justify-between items-center pt-2">
            <button disabled={offset === 0} onClick={() => load(offset - 20)}
              className="btn-outline text-xs disabled:opacity-30">← Prev</button>
            <button disabled={offset + 20 >= total} onClick={() => load(offset + 20)}
              className="btn-outline text-xs disabled:opacity-30">Next →</button>
          </div>
        </>
      )}

      {selectedId && (
        <FailureDetail failureId={selectedId} onClose={() => setSelectedId(null)} />
      )}
    </div>
  )
}