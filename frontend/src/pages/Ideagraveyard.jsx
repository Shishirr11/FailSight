import { useState, useEffect, useCallback } from 'react'
import { Search, SlidersHorizontal, X, RefreshCw, Skull, ArrowLeft } from 'lucide-react'
import { fetchFailures, fetchFailureStats, fetchFailure } from '../api/failures'

const fmt = n =>
  !n || n === 0 ? null
  : n >= 1e9 ? `$${(n/1e9).toFixed(1)}B`
  : n >= 1e6 ? `$${(n/1e6).toFixed(1)}M`
  : n >= 1e3 ? `$${(n/1e3).toFixed(0)}K`
  : `$${Math.round(n)}`

const REASON_LABELS = {
  no_market: 'No market', ran_out_of_money: 'Ran out of money',
  competition: 'Competition', bad_timing: 'Bad timing',
  team_issues: 'Team issues', product_issues: 'Product issues',
  pricing: 'Pricing', regulation: 'Regulation',
  pivot_failed: 'Pivot failed', fundraising: 'Fundraising',
}

const SECTORS = [
  'AI & Machine Learning','Cybersecurity','Clean Energy','Climate Technology',
  'Biotechnology','Health Technology','Quantum Computing','Advanced Manufacturing',
  'Aerospace & Defense','Agriculture Technology','Advanced Computing','Fintech',
]

function Spinner({ size = 18 }) {
  return <RefreshCw size={size} className="animate-spin text-grey-400" />
}

function ReasonChip({ reason }) {
  return (
    <span className="text-xs px-2.5 py-1 rounded-full bg-red-50 text-red-600 border border-red-100 font-semibold">
      {REASON_LABELS[reason] || reason?.replace(/_/g, ' ')}
    </span>
  )
}

function FilterPanel({ filters, onApply, onClose }) {
  const [local, setLocal] = useState(filters)
  const set = (k, v) => setLocal(p => ({ ...p, [k]: v }))
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/20"
         onClick={e => e.target === e.currentTarget && onClose()}>
      <div className="bg-white rounded-2xl shadow-lg border border-grey-200 w-[460px] animate-pop-in">
        <div className="flex items-center justify-between px-6 py-5 border-b border-grey-100">
          <p className="font-bold text-grey-900 text-lg">Filter graveyard</p>
          <button onClick={onClose} className="btn-ghost p-2"><X size={16} /></button>
        </div>
        <div className="px-6 py-5 space-y-5">
          <div>
            <p className="section-label mb-2.5">Sector</p>
            <div className="flex flex-wrap gap-2">
              {SECTORS.map(s => (
                <button key={s} onClick={() => set('sector', local.sector === s ? '' : s)}
                  className={`chip text-sm font-semibold transition-colors
                    ${local.sector === s ? 'chip-green' : 'chip-grey hover:border-grey-300'}`}>
                  {s}
                </button>
              ))}
            </div>
          </div>
          <div>
            <p className="section-label mb-2.5">Failure reason</p>
            <select className="input text-base" value={local.reason || ''}
              onChange={e => set('reason', e.target.value)}>
              <option value="">All reasons</option>
              {Object.entries(REASON_LABELS).map(([k, v]) => (
                <option key={k} value={k}>{v}</option>
              ))}
            </select>
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <p className="section-label mb-2">Year from</p>
              <input className="input text-base" type="number" value={local.minYear || ''}
                onChange={e => set('minYear', e.target.value ? Number(e.target.value) : null)}
                placeholder="e.g. 2018" />
            </div>
            <div>
              <p className="section-label mb-2">Year to</p>
              <input className="input text-base" type="number" value={local.maxYear || ''}
                onChange={e => set('maxYear', e.target.value ? Number(e.target.value) : null)}
                placeholder="e.g. 2024" />
            </div>
          </div>
          <div>
            <p className="section-label mb-2">Min funding raised ($)</p>
            <input className="input text-base" type="number" value={local.minFunding || ''}
              onChange={e => set('minFunding', e.target.value ? Number(e.target.value) : null)}
              placeholder="e.g. 1000000" />
          </div>
        </div>
        <div className="flex gap-2 px-6 py-4 border-t border-grey-100">
          <button onClick={() => { setLocal({}); onApply({}) }} className="btn-ghost text-sm">Remove all filters</button>
          <div className="flex-1" />
          <button onClick={onClose} className="btn-secondary text-sm">Cancel</button>
          <button onClick={() => { onApply(local); onClose() }} className="btn-primary text-sm">Apply</button>
        </div>
      </div>
    </div>
  )
}

function FailureCard({ failure, onClick }) {
  const outcomeStyle = {
    Bankruptcy: 'bg-red-50 text-red-600 border-red-100',
    Acquired:   'bg-blue-50 text-blue-600 border-blue-100',
    Shutdown:   'bg-grey-100 text-grey-600 border-grey-200',
  }[failure.outcome] || 'bg-grey-100 text-grey-600 border-grey-200'

  return (
    <button onClick={() => onClick(failure)}
      className="w-full text-left bg-white border border-grey-200 rounded-xl p-4
                 hover:border-grey-300 hover:shadow-sm transition-all">
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-2 flex-wrap">
            <span className="font-bold text-base text-grey-900">{failure.company_name}</span>
            {failure.sector && (
              <span className="text-xs px-2.5 py-1 rounded-full bg-grey-100 text-grey-500 border border-grey-200 font-medium">
                {failure.sector}
              </span>
            )}
            {failure.outcome && (
              <span className={`text-xs px-2.5 py-1 rounded-full border font-semibold ${outcomeStyle}`}>
                {failure.outcome}
              </span>
            )}
          </div>
          {failure.description && (
            <p className="text-sm text-grey-500 mb-2.5 line-clamp-2">{failure.description}</p>
          )}
          <div className="flex flex-wrap gap-1.5">
            {(failure.failure_reasons || []).slice(0, 4).map(r => (
              <ReasonChip key={r} reason={r} />
            ))}
          </div>
          {failure.key_lesson && (
            <p className="text-sm text-amber-700 mt-2 italic line-clamp-1">💡 {failure.key_lesson}</p>
          )}
        </div>
        <div className="text-right shrink-0">
          {failure.funding_raised_usd > 0 && (
            <p className="text-base font-bold text-red-500">{fmt(failure.funding_raised_usd)}</p>
          )}
          {failure.year_failed && (
            <p className="text-xs text-grey-400 mt-0.5 font-medium">
              {failure.year_founded && `${failure.year_founded}–`}{failure.year_failed}
            </p>
          )}
        </div>
      </div>
    </button>
  )
}

function FailureDetail({ failureId, onNameLoad }) {
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(true)
  useEffect(() => {
    setLoading(true)
    fetchFailure(failureId)
      .then(d => { setData(d); onNameLoad?.(d?.company_name) })
      .catch(() => setData(null))
      .finally(() => setLoading(false))
  }, [failureId])

  if (loading) return <div className="flex items-center justify-center h-64"><Spinner size={22} /></div>
  if (!data) return null

  return (
    <div className="bg-white border border-grey-200 rounded-2xl overflow-hidden animate-fade-in">
      <div className="flex items-center justify-between px-6 py-5 border-b border-grey-100">
        <h2 className="font-display text-xl font-bold text-grey-900">{data.company_name}</h2>
        {data.outcome && <span className="chip chip-grey text-sm font-semibold">{data.outcome}</span>}
      </div>

      <div className="p-6 space-y-5">
        <div className="grid grid-cols-3 gap-3">
          {data.funding_raised_usd > 0 && (
            <div className="bg-red-50 rounded-xl p-4 border border-red-100 text-center">
              <p className="section-label mb-1.5">Raised</p>
              <p className="text-xl font-bold text-red-600">{fmt(data.funding_raised_usd)}</p>
            </div>
          )}
          {data.year_failed && (
            <div className="bg-grey-50 rounded-xl p-4 border border-grey-100 text-center">
              <p className="section-label mb-1.5">Failed</p>
              <p className="text-xl font-bold text-grey-700">{data.year_failed}</p>
            </div>
          )}
          {data.year_founded && (
            <div className="bg-grey-50 rounded-xl p-4 border border-grey-100 text-center">
              <p className="section-label mb-1.5">Founded</p>
              <p className="text-xl font-bold text-grey-700">{data.year_founded}</p>
            </div>
          )}
        </div>

        {data.sector && (
          <div>
            <p className="section-label mb-2">Sector</p>
            <span className="chip chip-grey text-sm font-semibold">{data.sector}</span>
          </div>
        )}

        {data.failure_reasons?.length > 0 && (
          <div>
            <p className="section-label mb-2.5">Failure reasons</p>
            <div className="flex flex-wrap gap-2">
              {data.failure_reasons.map(r => <ReasonChip key={r} reason={r} />)}
            </div>
          </div>
        )}

        {(data.why_failed || data.description) && (
          <div>
            <p className="section-label mb-2">What happened</p>
            <p className="text-base text-grey-700 leading-relaxed">{data.why_failed || data.description}</p>
          </div>
        )}

        {data.key_lesson && (
          <div className="bg-amber-50 border border-amber-100 rounded-xl p-5">
            <div className="flex items-center gap-2 mb-2.5">
              <span className="text-lg">💡</span>
              <p className="text-xs font-bold text-amber-700 uppercase tracking-wider">Key lesson</p>
            </div>
            <p className="text-base text-amber-900 leading-relaxed">{data.key_lesson}</p>
          </div>
        )}

        {data.market_analysis && (
          <div>
            <p className="section-label mb-2">Market analysis</p>
            <p className="text-base text-grey-700 leading-relaxed">{data.market_analysis}</p>
          </div>
        )}

        {(data.difficulty || data.scalability || data.market_potential) && (
          <div>
            <p className="section-label mb-2.5">Lootdrop scores</p>
            <div className="grid grid-cols-3 gap-2.5">
              {[
                ['Difficulty',  data.difficulty,       'text-red-600'],
                ['Scalability', data.scalability,      'text-blue-600'],
                ['Market',      data.market_potential, 'text-green-600'],
              ].filter(([, v]) => v).map(([label, val, color]) => (
                <div key={label} className="bg-grey-50 rounded-xl p-3 border border-grey-100 text-center">
                  <p className="text-xs text-grey-400 mb-1 font-semibold">{label}</p>
                  <p className={`text-2xl font-bold ${color}`}>{val}</p>
                </div>
              ))}
            </div>
          </div>
        )}

        {data.founder_names && (
          <div>
            <p className="section-label mb-2">Founders</p>
            <p className="text-base text-grey-700 font-medium">{data.founder_names}</p>
          </div>
        )}

        {data.source_url && (
          <a href={data.source_url} target="_blank" rel="noopener noreferrer"
             className="btn-outline-green text-sm font-semibold inline-flex items-center gap-1.5">
            Read full story ↗
          </a>
        )}
      </div>
    </div>
  )
}

export default function IdeaGraveyard() {
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

  const load = useCallback(async (query, flt, off = 0) => {
    setLoading(true)
    try {
      const data = await fetchFailures({
        q: query, sector: flt?.sector || '', reason: flt?.reason || '',
        minYear: flt?.minYear || null, maxYear: flt?.maxYear || null,
        limit: LIMIT, offset: off,
      })
      setResults(data.results || [])
      setTotal(data.total || 0)
    } catch (e) { console.error(e) }
    finally { setLoading(false) }
  }, [])

  useEffect(() => { load('', {}) }, [load])

  useEffect(() => {
    if (!q.trim()) {
      const t = setTimeout(() => load('', filters, 0), 300)
      return () => clearTimeout(t)
    }
  }, [q]) 

  const handleSearch = () => { setOffset(0); setSelected(null); load(q, filters, 0) }
  const handleApplyFilters = flt => { setFilters(flt); setOffset(0); setSelected(null); setSelectedName(''); load(q, flt, 0) }
  const activeCount = Object.values(filters).filter(v => v && v !== '').length

  if (selected) {
    const breadcrumb = q ? `"${q}"` : filters.sector || 'All failures'
    return (
      <div className="flex flex-col h-screen overflow-hidden bg-grey-50 animate-fade-in">
        <div className="bg-white border-b border-grey-200 px-5 py-3.5 shrink-0 flex items-center gap-4">
          <button
            onClick={() => { setSelected(null); setSelectedName('') }}
            className="flex items-center gap-2 text-sm font-bold text-grey-600
                       hover:text-grey-900 hover:bg-grey-100 px-3 py-2 rounded-lg transition-colors shrink-0"
          >
            <ArrowLeft size={15} />
            Back to results
          </button>
          <div className="w-px h-5 bg-grey-200 shrink-0" />
          <div className="flex items-center gap-2 min-w-0 text-sm">
            {/* <img src="/brain-logo.png" alt="" className="w-5 h-5 object-contain opacity-40 shrink-0" /> */}
            <span className="font-semibold text-grey-400 shrink-0">Idea Graveyard</span>
            <span className="text-grey-300 shrink-0 mx-0.5">›</span>
            <span className="font-semibold text-grey-500 shrink-0">{breadcrumb}</span>
            {selectedName && (
              <>
                <span className="text-grey-300 shrink-0 mx-0.5">›</span>
                <span className="font-bold text-grey-900 truncate">{selectedName}</span>
              </>
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
    <div className="flex flex-col h-screen overflow-hidden bg-grey-50">

      <div className="bg-white border-b border-grey-200 px-5 py-4 shrink-0">
        <div className="flex items-center gap-4 mb-4">
          {/* <img src="/brain-logo.png" alt="" className="w-9 h-9 object-contain" /> */}
          <div>
            <h1 className="font-display text-2xl font-bold text-grey-2000 italic"> The Graveyard</h1>
            <p className="text-sm text-grey-400 font-medium">
              {stats?.total?.toLocaleString() || '—'} documented failures
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
            <input className="input pl-10 h-11 text-base font-medium"
              value={q} onChange={e => setQ(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleSearch()}
              placeholder="Search by company, sector, lesson…" />
            {q && (
              <button onClick={() => setQ('')} className="absolute right-3 top-1/2 -translate-y-1/2 text-grey-400 hover:text-grey-600">
                <X size={14} />
              </button>
            )}
          </div>
          <button onClick={() => setShowFilter(true)}
            className={`btn-secondary h-11 px-4 text-sm font-semibold flex items-center gap-2 relative
              ${activeCount > 0 ? 'border-green-300 text-green-700' : ''}`}>
            <SlidersHorizontal size={15} />
            Filter
            {activeCount > 0 && (
              <span className="absolute -top-1.5 -right-1.5 w-5 h-5 bg-green-600 text-white
                               text-xs rounded-full flex items-center justify-center font-bold">
                {activeCount}
              </span>
            )}
          </button>
          <button onClick={handleSearch} className="btn-primary h-11 px-5 text-sm font-bold">Search</button>
        </div>

        {activeCount > 0 && (
          <div className="flex flex-wrap gap-2 mt-3">
            {filters.sector && (
              <span className="chip chip-green text-sm font-semibold flex items-center gap-1.5">
                {filters.sector}
                <button onClick={() => handleApplyFilters({ ...filters, sector: '' })}><X size={10} /></button>
              </span>
            )}
            {filters.reason && (
              <span className="chip chip-red text-sm font-semibold flex items-center gap-1.5">
                {REASON_LABELS[filters.reason] || filters.reason}
                <button onClick={() => handleApplyFilters({ ...filters, reason: '' })}><X size={10} /></button>
              </span>
            )}
            {(filters.minYear || filters.maxYear) && (
              <span className="chip chip-grey text-sm font-semibold flex items-center gap-1.5">
                {filters.minYear || '?'}–{filters.maxYear || 'now'}
                <button onClick={() => handleApplyFilters({ ...filters, minYear: null, maxYear: null })}><X size={10} /></button>
              </span>
            )}
            <button onClick={() => handleApplyFilters({})} className="text-sm text-grey-400 hover:text-grey-600 underline font-medium ml-1">
              Clear all
            </button>
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
              <button disabled={offset === 0}
                onClick={() => { const o = Math.max(0, offset - LIMIT); setOffset(o); load(q, filters, o) }}
                className="btn-secondary text-sm font-semibold disabled:opacity-40">← Prev</button>
              <button disabled={offset + LIMIT >= total}
                onClick={() => { const o = offset + LIMIT; setOffset(o); load(q, filters, o) }}
                className="btn-secondary text-sm font-semibold disabled:opacity-40">Next →</button>
            </div>
          )}
        </div>
      </div>

      {showFilter && (
        <FilterPanel filters={filters} onApply={handleApplyFilters} onClose={() => setShowFilter(false)} />
      )}
    </div>
  )
}