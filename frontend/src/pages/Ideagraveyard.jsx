import { useState, useEffect, useCallback } from 'react'
import { useLocation } from 'react-router-dom'
import { Skull, Search, SlidersHorizontal, X, ArrowLeft, RefreshCw, ExternalLink } from 'lucide-react'
import { fetchFailures, fetchFailureStats, fetchFailure } from '../api/failures'

const fmt = n =>
  !n || n === 0 ? null
  : n >= 1e9 ? `$${(n/1e9).toFixed(1)}B`
  : n >= 1e6 ? `$${(n/1e6).toFixed(1)}M`
  : n >= 1e3 ? `$${(n/1e3).toFixed(0)}K`
  : `$${Math.round(n)}`

// Strip AI-generated markdown artifacts from stored text
function cleanText(text) {
  if (!text || typeof text !== 'string') return ''
  return text
    // Remove **bold** and *italic* markers
    .replace(/\*\*(.+?)\*\*/g, '$1')
    .replace(/\*(.+?)\*/g, '$1')
    // Remove lines that are just a dash-bullet list item — convert to prose flow
    .replace(/^[\-•]\s+/gm, '')
    // Remove ### headings — just leave the text
    .replace(/^#{1,3}\s+/gm, '')
    // Collapse 3+ newlines to a paragraph break
    .replace(/\n{3,}/g, '\n\n')
    // Remove trailing/leading whitespace per line
    .split('\n').map(l => l.trim()).join('\n')
    .trim()
}

const REASON_LABELS = {
  no_pmf:              'No Product-Market Fit',
  cash:                'Ran out of cash',
  competition:         'Competition',
  competition_giants:  'Giant competitors',
  acquisition:         'Acquisition stagnation',
  platform_dependency: 'Platform dependency',
  high_costs:          'High operational costs',
  monetization:        'Monetization failure',
  niche:               'Niche limits',
  execution:           'Execution flaws',
  trend_shift:         'Trend shifted',
  trust:               'Trust / toxicity issues',
  regulatory:          'Regulatory pressure',
  overhype:            'Overhype',
}

function Spinner({ size = 18 }) {
  return <RefreshCw size={size} className="animate-spin text-grey-400" />
}

function ReasonChip({ reason }) {
  return (
    <span className="chip chip-red text-xs">
      {REASON_LABELS[reason] || reason.replace(/_/g, ' ')}
    </span>
  )
}

function FailureCard({ failure, onClick }) {
  const reasons = Array.isArray(failure.failure_reasons) ? failure.failure_reasons : []
  return (
    <div onClick={() => onClick(failure)}
      className="bg-white border border-grey-200 rounded-xl p-4 cursor-pointer
                 hover:border-grey-300 hover:shadow-sm transition-all duration-150 group">
      <div className="flex items-start justify-between gap-2 mb-2">
        <div className="flex-1 min-w-0">
          <p className="text-base font-bold text-grey-900 leading-snug truncate group-hover:text-navy-900">
            {failure.company_name}
          </p>
          {failure.sector && <p className="text-sm text-grey-400 mt-0.5 font-medium">{failure.sector}</p>}
        </div>
        <div className="text-right shrink-0">
          {failure.year_failed && <p className="text-sm font-bold text-grey-600">{failure.year_failed}</p>}
          {fmt(failure.funding_raised_usd) && (
            <p className="text-xs font-bold text-red-600 mt-0.5">{fmt(failure.funding_raised_usd)} raised</p>
          )}
        </div>
      </div>
      {reasons.length > 0 && (
        <div className="flex flex-wrap gap-1.5 mt-2">
          {reasons.slice(0, 3).map(r => <ReasonChip key={r} reason={r} />)}
        </div>
      )}
      {failure.key_lesson && (
        <p className="text-sm text-grey-500 italic mt-2.5 line-clamp-1">
          {cleanText(failure.key_lesson)}
        </p>
      )}
    </div>
  )
}

function FilterPanel({ filters, onApply, onClose }) {
  const [local, setLocal] = useState(filters)
  const set = (k, v) => setLocal(p => ({ ...p, [k]: v }))
  const REASONS = Object.keys(REASON_LABELS)
  const SECTORS = [
    'AI & Machine Learning', 'Health Technology', 'Fintech', 'E-commerce',
    'Biotechnology', 'Clean Energy', 'Cybersecurity', 'Advanced Computing',
    'Education', 'Transportation', 'Aerospace & Defense',
  ]
  return (
    <div className="fixed inset-0 z-50 flex items-end sm:items-center justify-center bg-black/30"
         onClick={e => e.target === e.currentTarget && onClose()}>
      <div className="bg-white rounded-2xl shadow-xl border border-grey-200 w-full max-w-md mx-4 animate-slide-up">
        <div className="flex items-center justify-between px-6 py-5 border-b border-grey-100">
          <p className="font-bold text-grey-900 text-lg">Filter failures</p>
          <button onClick={onClose} className="btn-ghost p-2"><X size={16} /></button>
        </div>
        <div className="px-6 py-5 space-y-5 max-h-[60vh] overflow-y-auto">
          <div>
            <p className="section-label mb-3">Sector</p>
            <div className="flex flex-wrap gap-2">
              {SECTORS.map(s => (
                <button key={s} onClick={() => set('sector', local.sector === s ? '' : s)}
                  className={`chip text-xs transition-colors ${local.sector === s ? 'chip-navy' : 'chip-grey hover:border-grey-300'}`}>
                  {s}
                </button>
              ))}
            </div>
          </div>
          <div>
            <p className="section-label mb-3">Failure reason</p>
            <div className="flex flex-wrap gap-2">
              {REASONS.map(r => (
                <button key={r} onClick={() => set('reason', local.reason === r ? '' : r)}
                  className={`chip text-xs transition-colors ${local.reason === r ? 'chip-red' : 'chip-grey hover:border-grey-300'}`}>
                  {REASON_LABELS[r]}
                </button>
              ))}
            </div>
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <p className="section-label mb-2">From year</p>
              <input className="input" type="number" min="1990" max="2025"
                value={local.minYear || ''} onChange={e => set('minYear', e.target.value ? Number(e.target.value) : null)}
                placeholder="e.g. 2015" />
            </div>
            <div>
              <p className="section-label mb-2">To year</p>
              <input className="input" type="number" min="1990" max="2025"
                value={local.maxYear || ''} onChange={e => set('maxYear', e.target.value ? Number(e.target.value) : null)}
                placeholder="e.g. 2023" />
            </div>
          </div>
        </div>
        <div className="flex gap-2 px-6 py-4 border-t border-grey-100">
          <button onClick={() => { setLocal({}); onApply({}) }} className="btn-ghost text-sm">Clear all</button>
          <div className="flex-1" />
          <button onClick={onClose} className="btn-secondary text-sm">Cancel</button>
          <button onClick={() => { onApply(local); onClose() }} className="btn-primary text-sm">Apply</button>
        </div>
      </div>
    </div>
  )
}

// ── Prose block — clean and display a stored text field ─────────────────────
function ProseBlock({ text }) {
  const cleaned = cleanText(text)
  if (!cleaned) return null
  return (
    <>
      {cleaned.split('\n\n').filter(Boolean).map((para, i) => (
        <p key={i} className="text-base text-grey-700 leading-relaxed">{para}</p>
      ))}
    </>
  )
}

function FailureDetail({ failureId, onNameLoad }) {
  const [data, setData]     = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (!failureId) return
    setLoading(true)
    fetchFailure(failureId)
      .then(d => { setData(d); onNameLoad?.(d.company_name) })
      .catch(() => setData(null))
      .finally(() => setLoading(false))
  }, [failureId])

  if (loading) return <div className="flex items-center justify-center h-48"><Spinner size={20} /></div>
  if (!data)   return <div className="text-base text-red-500 p-6">Could not load failure details.</div>

  const raw = (() => { try { return JSON.parse(data.raw_json || '{}') } catch { return {} } })()
  const why_failed       = data.why_failed       || raw.why_failed       || ''
  const description      = data.description      || raw.description      || ''
  const market_analysis  = data.market_analysis  || raw.market_analysis  || raw._detail_market_analysis || ''
  const difficulty       = data.difficulty       || raw.difficulty       || ''
  const scalability      = data.scalability      || raw.scalability      || ''
  const market_potential = data.market_potential || raw.market_potential || ''

  return (
    <div className="space-y-5">
      {/* Hero */}
      <div className="bg-white border border-grey-200 rounded-2xl p-6 shadow-sm">
        <div className="flex items-start justify-between gap-4 mb-4">
          <div>
            <h2 className="text-2xl font-bold text-grey-900 mb-1">{data.company_name}</h2>
            <div className="flex items-center gap-3 text-sm text-grey-500 font-medium">
              {data.year_founded && <span>Founded {data.year_founded}</span>}
              {data.year_failed  && <><span className="text-grey-300">→</span><span>Failed {data.year_failed}</span></>}
            </div>
          </div>
          {fmt(data.funding_raised_usd) && (
            <div className="shrink-0 bg-red-50 border border-red-100 rounded-xl px-4 py-3 text-center">
              <p className="text-2xs font-bold uppercase tracking-wider text-red-500 mb-1">Raised</p>
              <p className="text-xl font-bold text-red-700">{fmt(data.funding_raised_usd)}</p>
            </div>
          )}
        </div>
        {data.sector && <div className="mb-4"><span className="chip chip-grey text-sm">{data.sector}</span></div>}
        {data.failure_reasons?.length > 0 && (
          <div>
            <p className="section-label mb-2.5">Failure reasons</p>
            <div className="flex flex-wrap gap-2">
              {data.failure_reasons.map(r => <ReasonChip key={r} reason={r} />)}
            </div>
          </div>
        )}
      </div>

      {/* What happened */}
      {(why_failed || description) && (
        <div className="bg-white border border-grey-200 rounded-2xl p-6 shadow-sm space-y-3">
          <p className="section-label">What happened</p>
          <ProseBlock text={why_failed || description} />
        </div>
      )}

      {/* Key lesson */}
      {data.key_lesson && (
        <div className="bg-amber-50 border border-amber-100 rounded-2xl p-6 shadow-sm">
          <div className="flex items-center gap-2 mb-3">
            <span className="text-base">💡</span>
            <p className="text-sm font-bold text-amber-700 uppercase tracking-wider">Key lesson</p>
          </div>
          <ProseBlock text={data.key_lesson} />
        </div>
      )}

      {/* Market analysis */}
      {market_analysis && (
        <div className="bg-white border border-grey-200 rounded-2xl p-6 shadow-sm space-y-3">
          <p className="section-label">Market context</p>
          <ProseBlock text={market_analysis} />
        </div>
      )}

      {/* Scores */}
      {(difficulty || scalability || market_potential) && (
        <div className="bg-white border border-grey-200 rounded-2xl p-6 shadow-sm">
          <p className="section-label mb-3">Lootdrop scores</p>
          <div className="grid grid-cols-3 gap-3">
            {[['Difficulty', difficulty, 'text-red-600'],['Scalability', scalability, 'text-blue-600'],['Market', market_potential, 'text-navy-800']].filter(([, v]) => v).map(([label, val, color]) => (
              <div key={label} className="bg-grey-50 rounded-xl p-3 border border-grey-100 text-center">
                <p className="text-xs text-grey-400 mb-1 font-semibold">{label}</p>
                <p className={`text-2xl font-bold ${color}`}>{val}</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Founders */}
      {data.founder_names && (
        <div className="bg-white border border-grey-200 rounded-2xl p-6 shadow-sm">
          <p className="section-label mb-2">Founders</p>
          <p className="text-base text-grey-700 font-medium">{data.founder_names}</p>
        </div>
      )}

      {data.source_url && (
        <a href={data.source_url} target="_blank" rel="noopener noreferrer"
           className="inline-flex items-center gap-2 btn-outline-navy text-sm font-semibold">
          <ExternalLink size={14} />
          Read full story
        </a>
      )}
    </div>
  )
}

// ── Main Page ───────────────────────────────────────────────────────────────
export default function IdeaGraveyard() {
  const location = useLocation()

  const [q, setQ]                   = useState('')
  const [filters, setFilters]       = useState({})
  const [showFilter, setShowFilter] = useState(false)
  const [results, setResults]       = useState([])
  const [total, setTotal]           = useState(0)
  const [loading, setLoading]       = useState(false)
  const [offset, setOffset]         = useState(0)
  const [stats, setStats]           = useState(null)
  const [selected, setSelected]     = useState(null)
  const [selectedName, setSelectedName] = useState('')
  const LIMIT = 20

  useEffect(() => { fetchFailureStats().then(setStats).catch(() => {}) }, [])

  useEffect(() => {
    const state = location.state
    if (!state?.openFailureId) return
    window.history.replaceState({}, '')
    setSelected(state.openFailureId)
  }, [location.state?.openFailureId])

  const load = useCallback(async (query, flt, off = 0) => {
    setLoading(true)
    try {
      const data = await fetchFailures({
        q: query, sector: flt?.sector || '', reason: flt?.reason || '',
        minYear: flt?.minYear || null, maxYear: flt?.maxYear || null,
        limit: LIMIT, offset: off,
      })
      setResults(data.results || []); setTotal(data.total || 0)
    } catch (e) { console.error(e) } finally { setLoading(false) }
  }, [])

  useEffect(() => { load('', {}) }, [load])
  useEffect(() => {
    if (!q.trim()) { const t = setTimeout(() => load('', filters, 0), 300); return () => clearTimeout(t) }
  }, [q])

  const handleSearch      = () => { setOffset(0); setSelected(null); load(q, filters, 0) }
  const handleApplyFilters = flt => { setFilters(flt); setOffset(0); setSelected(null); setSelectedName(''); load(q, flt, 0) }
  const activeCount = Object.values(filters).filter(v => v && v !== '').length

  if (selected) {
    const breadcrumb = q ? `"${q}"` : filters.sector || 'All failures'
    return (
      <div className="flex flex-col h-screen overflow-hidden bg-grey-100 animate-fade-in">
        <div className="bg-white border-b border-grey-200 px-5 py-3.5 shrink-0 flex items-center gap-3">
          <button onClick={() => { setSelected(null); setSelectedName('') }}
            className="flex items-center gap-2 text-sm font-bold text-grey-600
                       hover:text-grey-900 hover:bg-grey-100 px-3 py-2 rounded-lg transition-colors shrink-0">
            <ArrowLeft size={15} /> Back to results
          </button>
          <div className="w-px h-5 bg-grey-200 shrink-0" />
          <div className="flex items-center gap-2 min-w-0 text-sm">
            <span className="font-semibold text-grey-400 shrink-0">Graveyard</span>
            <span className="text-grey-300 shrink-0">›</span>
            <span className="font-semibold text-grey-500 shrink-0">{breadcrumb}</span>
            {selectedName && (
              <><span className="text-grey-300 shrink-0">›</span>
              <span className="font-bold text-grey-900 truncate">{selectedName}</span></>
            )}
          </div>
        </div>
        <div className="flex-1 overflow-y-auto px-5 py-6">
          <div className="max-w-3xl mx-auto">
            <FailureDetail failureId={selected} onNameLoad={name => setSelectedName(name || '')} />
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex flex-col h-screen overflow-hidden bg-grey-100">

      <div className="bg-white border-b border-grey-200 px-5 py-4 shrink-0">
        <div className="flex items-center gap-4 mb-4">
          <div>
            <h1 className="font-display text-2xl font-bold text-grey-900 italic">The Graveyard</h1>
            <p className="text-sm text-grey-400 font-medium">
              {stats?.total?.toLocaleString() || '—'} documented startup failures
            </p>
          </div>
          {stats && (
            <div className="ml-auto flex items-center gap-5 text-sm text-grey-500">
              <span>Avg raised: <b className="text-red-600 font-bold">{fmt(stats.avg_funding_burned) || '—'}</b></span>
              {stats.top_reasons?.[0] && (
                <span>Top reason: <b className="text-grey-700 font-bold">{REASON_LABELS[stats.top_reasons[0].reason] || stats.top_reasons[0].reason}</b></span>
              )}
            </div>
          )}
        </div>

        <div className="flex gap-3">
          <div className="relative flex-1">
            <Search size={15} className="absolute left-3.5 top-1/2 -translate-y-1/2 text-grey-400 pointer-events-none" />
            <input className="input pl-10 h-11" value={q} onChange={e => setQ(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleSearch()}
              placeholder="Search by company, sector, lesson…" />
            {q && <button onClick={() => setQ('')} className="absolute right-3 top-1/2 -translate-y-1/2 text-grey-400 hover:text-grey-600"><X size={14} /></button>}
          </div>
          <button onClick={() => setShowFilter(true)}
            className={`btn-secondary h-11 px-4 flex items-center gap-2 relative ${activeCount > 0 ? 'border-navy-300 text-navy-800' : ''}`}>
            <SlidersHorizontal size={15} />Filter
            {activeCount > 0 && <span className="absolute -top-1.5 -right-1.5 w-5 h-5 bg-navy-900 text-white text-xs rounded-full flex items-center justify-center font-bold">{activeCount}</span>}
          </button>
          <button onClick={handleSearch} className="btn-primary h-11 px-5 flex items-center gap-2"><Search size={14} />Search</button>
        </div>

        {activeCount > 0 && (
          <div className="flex flex-wrap gap-2 mt-3">
            {filters.sector && (
              <span className="chip chip-navy flex items-center gap-1.5 text-sm">
                {filters.sector}
                <button onClick={() => handleApplyFilters({ ...filters, sector: '' })}><X size={10} /></button>
              </span>
            )}
            {filters.reason && (
              <span className="chip chip-red flex items-center gap-1.5 text-sm">
                {REASON_LABELS[filters.reason] || filters.reason}
                <button onClick={() => handleApplyFilters({ ...filters, reason: '' })}><X size={10} /></button>
              </span>
            )}
            {(filters.minYear || filters.maxYear) && (
              <span className="chip chip-grey flex items-center gap-1.5 text-sm">
                {filters.minYear || '?'}–{filters.maxYear || 'now'}
                <button onClick={() => handleApplyFilters({ ...filters, minYear: null, maxYear: null })}><X size={10} /></button>
              </span>
            )}
            <button onClick={() => handleApplyFilters({})} className="text-sm text-grey-400 hover:text-grey-600 underline font-medium ml-1">Clear all</button>
          </div>
        )}
      </div>

      <div className="flex-1 overflow-y-auto px-5 py-5">
        <div className="max-w-3xl mx-auto">
          {!loading && results.length > 0 && (
            <p className="text-sm text-grey-400 mb-3 font-medium">
              Showing {offset + 1}–{Math.min(offset + results.length, total)} of {total.toLocaleString()} failures
            </p>
          )}
          {loading ? (
            <div className="flex items-center justify-center h-48"><Spinner size={20} /></div>
          ) : results.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-48 text-center">
              <Skull size={36} className="text-grey-300 mb-3" />
              <p className="text-base font-semibold text-grey-500">No failures found</p>
              <p className="text-sm text-grey-400 mt-1">Try adjusting your search or filters</p>
            </div>
          ) : (
            <div className="space-y-2.5">
              {results.map(f => (
                <FailureCard key={f.failure_id} failure={f} onClick={f => { setSelectedName(''); setSelected(f.failure_id) }} />
              ))}
            </div>
          )}
          {total > LIMIT && !loading && (
            <div className="flex justify-between items-center mt-5">
              <button disabled={offset === 0} onClick={() => { const o = Math.max(0, offset - LIMIT); setOffset(o); load(q, filters, o) }} className="btn-secondary text-sm disabled:opacity-40">← Prev</button>
              <span className="text-sm text-grey-400 font-medium">Page {Math.floor(offset / LIMIT) + 1} / {Math.ceil(total / LIMIT)}</span>
              <button disabled={offset + LIMIT >= total} onClick={() => { const o = offset + LIMIT; setOffset(o); load(q, filters, o) }} className="btn-secondary text-sm disabled:opacity-40">Next →</button>
            </div>
          )}
        </div>
      </div>

      {showFilter && <FilterPanel filters={filters} onApply={handleApplyFilters} onClose={() => setShowFilter(false)} />}
    </div>
  )
}