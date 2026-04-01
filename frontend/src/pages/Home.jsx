import { useState, useEffect, useRef, useCallback } from 'react'
import {
  Search, SlidersHorizontal, X, ChevronDown, RefreshCw,
  Database, DollarSign, TrendingUp, Layers, ExternalLink,
  ArrowLeft, Sparkles, BarChart2, Clock, AlertTriangle,
  FileText, FlaskConical, Cpu, Microscope
} from 'lucide-react'
import { semanticSearch, fetchSuggestions } from '../api/search'
import { fetchOpportunities, fetchStats, fetchOpportunity } from '../api/opportunities'
import { fetchSectorBriefing, explainOpportunity } from '../api/briefings'
import { fetchPipelineStatus, triggerPipelineRun } from '../api/pipeline'


const TABS = [
  { key: 'grants',   label: 'Grants',   icon: FileText,    color: 'text-emerald-400' },
  { key: 'sam',      label: 'SAM',      icon: Layers,      color: 'text-indigo-400'  },
  { key: 'patents',  label: 'Patents',  icon: Cpu,         color: 'text-purple-400'  },
  { key: 'research', label: 'Research', icon: Microscope,  color: 'text-amber-400'   },
]

const SOURCE_BADGE = {
  grants:   'badge-grants',
  sam:      'badge-sam',
  patents:  'badge-patents',
  research: 'badge-research',
}

const fmt = n => !n || n === 0 ? null
  : n >= 1e9 ? `$${(n/1e9).toFixed(1)}B`
  : n >= 1e6 ? `$${(n/1e6).toFixed(1)}M`
  : n >= 1e3 ? `$${(n/1e3).toFixed(0)}K`
  : `$${n}`


function SourceBadge({ source }) {
  return <span className={SOURCE_BADGE[source] ?? 'badge-sam'}>{source}</span>
}

function Spinner({ size = 16 }) {
  return <RefreshCw size={size} className="animate-spin text-slate-500" />
}

function EmptyState({ text }) {
  return (
    <div className="flex flex-col items-center justify-center h-48 text-center px-6">
      <div className="w-10 h-10 rounded-xl bg-white/[0.04] flex items-center justify-center mb-3">
        <Sparkles size={18} className="text-slate-600" />
      </div>
      <p className="text-sm text-slate-500">{text}</p>
    </div>
  )
}


function DirectoryPanel({ stats, pipeline, onRefresh, refreshing }) {
  return (
    <div className="card px-4 py-3 flex items-center gap-6 flex-wrap">

      {}
      {stats ? (
        <>
          <div className="flex items-center gap-2">
            <Database size={13} className="text-slate-500" />
            <span className="text-xs text-slate-400">
              <span className="text-slate-100 font-semibold tabular-nums">
                {stats.total?.toLocaleString()}
              </span> opportunities
            </span>
          </div>
          {stats.by_source?.map(s => (
            <div key={s.source} className="flex items-center gap-1.5">
              <SourceBadge source={s.source} />
              <span className="text-xs text-slate-400 tabular-nums">{s.count?.toLocaleString()}</span>
            </div>
          ))}
          {stats.funding_range && (
            <div className="flex items-center gap-1.5">
              <DollarSign size={13} className="text-slate-500" />
              <span className="text-xs text-slate-400">
                {fmt(stats.funding_range.min)} – {fmt(stats.funding_range.max)}
              </span>
            </div>
          )}
        </>
      ) : (
        <Spinner />
      )}

      {}
      <button
        onClick={onRefresh}
        disabled={refreshing}
        className="ml-auto flex items-center gap-1.5 btn-outline py-1.5 px-3 text-xs"
        title="Fetch fresh data from all sources"
      >
        <RefreshCw size={12} className={refreshing ? 'animate-spin' : ''} />
        {refreshing ? 'Refreshing…' : 'Refresh Data'}
      </button>

      {}
      {pipeline?.last_run && (
        <span className="text-2xs text-slate-600 flex items-center gap-1">
          <Clock size={10} /> {new Date(pipeline.last_run).toLocaleDateString()}
        </span>
      )}
    </div>
  )
}


function SearchBar({ onSearch, loading }) {
  const [q, setQ]                   = useState('')
  const [minFunding, setMinFunding] = useState('')
  const [sector, setSector]         = useState('')
  const [showFilters, setShowFilters] = useState(false)
  const [suggestions, setSuggestions] = useState({ sectors: [], agencies: [] })
  const [showSuggest, setShowSuggest] = useState(false)
  const suggestTimer = useRef(null)

  const handleInput = e => {
    const v = e.target.value
    setQ(v)
    clearTimeout(suggestTimer.current)
    if (v.length >= 2) {
      suggestTimer.current = setTimeout(async () => {
        const s = await fetchSuggestions(v)
        setSuggestions(s)
        setShowSuggest(true)
      }, 250)
    } else {
      setShowSuggest(false)
    }
  }

  const submit = () => {
    setShowSuggest(false)
    onSearch({ q, minFunding: minFunding ? Number(minFunding) : null, sector })
  }

  return (
    <div className="space-y-2">
      <div className="flex gap-2">
        {}
        <div className="relative flex-1">
          <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500 pointer-events-none" />
          <input
            className="input pl-9 pr-4 py-2.5"
            value={q}
            onChange={handleInput}
            onKeyDown={e => e.key === 'Enter' && submit()}
            onFocus={() => suggestions.sectors.length && setShowSuggest(true)}
            onBlur={() => setTimeout(() => setShowSuggest(false), 150)}
            placeholder="Describe what you're looking for… e.g. 'AI grants under $500K closing soon'"
          />
          {}
          {showSuggest && (suggestions.sectors.length > 0 || suggestions.agencies.length > 0) && (
            <div className="absolute top-full left-0 right-0 mt-1 card shadow-card-md z-20 py-1 animate-fade-in">
              {suggestions.sectors.map(s => (
                <button key={s} onMouseDown={() => { setSector(s); setQ(s); setShowSuggest(false) }}
                  className="w-full text-left px-3 py-2 text-sm text-slate-300 hover:bg-white/[0.06] flex items-center gap-2">
                  <BarChart2 size={12} className="text-slate-500" /> {s}
                </button>
              ))}
              {suggestions.agencies.map(a => (
                <button key={a} onMouseDown={() => { setQ(a); setShowSuggest(false) }}
                  className="w-full text-left px-3 py-2 text-sm text-slate-300 hover:bg-white/[0.06] flex items-center gap-2">
                  <Database size={12} className="text-slate-500" /> {a}
                </button>
              ))}
            </div>
          )}
        </div>

        <button onClick={() => setShowFilters(f => !f)}
          className={`btn-outline flex items-center gap-1.5 ${showFilters ? 'border-brand-500/40 text-brand-400' : ''}`}>
          <SlidersHorizontal size={13} /> Filters
          <ChevronDown size={12} className={`transition-transform ${showFilters ? 'rotate-180' : ''}`} />
        </button>

        <button onClick={submit} disabled={loading}
          className="btn-primary flex items-center gap-2 min-w-[90px] justify-center">
          {loading ? <Spinner size={14} /> : <><Sparkles size={13} /> Search</>}
        </button>
      </div>

      {}
      {showFilters && (
        <div className="card p-3 grid grid-cols-2 gap-3 animate-slide-up">
          <div>
            <p className="section-label mb-1.5">Min Funding</p>
            <input className="input" type="number" value={minFunding}
              onChange={e => setMinFunding(e.target.value)} placeholder="e.g. 50000" />
          </div>
          <div>
            <p className="section-label mb-1.5">Sector</p>
            <input className="input" value={sector}
              onChange={e => setSector(e.target.value)} placeholder="e.g. AI & Machine Learning" />
          </div>
        </div>
      )}

      {}
      {(sector || minFunding) && (
        <div className="flex gap-2 flex-wrap">
          {sector && (
            <button onClick={() => setSector('')}
              className="flex items-center gap-1 text-xs px-2.5 py-1 rounded-full
                         bg-brand-600/10 text-brand-400 border border-brand-500/20">
              {sector} <X size={10} />
            </button>
          )}
          {minFunding && (
            <button onClick={() => setMinFunding('')}
              className="flex items-center gap-1 text-xs px-2.5 py-1 rounded-full
                         bg-brand-600/10 text-brand-400 border border-brand-500/20">
              Min {fmt(Number(minFunding))} <X size={10} />
            </button>
          )}
        </div>
      )}
    </div>
  )
}


function OppCard({ opp, onClick, active }) {
  return (
    <button onClick={() => onClick(opp)}
      className={`w-full text-left p-3.5 rounded-xl border transition-all duration-100
        ${active
          ? 'bg-brand-600/10 border-brand-500/40'
          : 'bg-surface-900 border-white/[0.06] hover:border-white/[0.12] hover:bg-surface-800'
        }`}
    >
      <div className="flex items-start justify-between gap-2 mb-1.5">
        <p className="text-sm font-semibold text-slate-100 leading-snug line-clamp-2 flex-1">
          {opp.title}
        </p>
        <SourceBadge source={opp.source} />
      </div>
      <div className="flex flex-wrap gap-x-3 gap-y-1 text-xs text-slate-500">
        {opp.agency    && <span className="truncate max-w-[140px]">{opp.agency}</span>}
        {opp.sector    && <span className="text-brand-400 font-medium">{opp.sector}</span>}
        {opp.funding_max > 0 && (
          <span className="text-emerald-400 font-semibold">↑ {fmt(opp.funding_max)}</span>
        )}
        {opp.close_date && <span>Closes {opp.close_date}</span>}
      </div>
    </button>
  )
}


function OpportunityPanel({ searchResults, searchQuery, onSelectOpp, selectedOpp }) {
  const [activeTab, setActiveTab]   = useState('grants')
  const [tabData, setTabData]       = useState({})
  const [tabLoading, setTabLoading] = useState({})
  const [offset, setOffset]         = useState(0)
  const [total, setTotal]           = useState(0)
  const [detailView, setDetailView] = useState(null)
  const [detailLoading, setDetailLoading] = useState(false)

  const usingSearch = searchResults !== null

  const loadTab = useCallback(async (source, off = 0) => {
    setTabLoading(p => ({ ...p, [source]: true }))
    try {
      const data = await fetchOpportunities({ source: [source], limit: 20, offset: off })
      setTabData(p => ({ ...p, [source]: data.results }))
      setTotal(data.total)
      setOffset(off)
    } finally {
      setTabLoading(p => ({ ...p, [source]: false }))
    }
  }, [])

  useEffect(() => {
    if (!usingSearch) loadTab(activeTab, 0)
  }, [activeTab, usingSearch])

  const handleCardClick = async opp => {
    onSelectOpp(opp)
    setDetailLoading(true)
    try {
      const full = await fetchOpportunity(opp.opp_id)
      setDetailView(full)
    } catch {
      setDetailView(opp)
    } finally {
      setDetailLoading(false)
    }
  }

  const displayList = usingSearch
    ? searchResults
    : (tabData[activeTab] || [])

  const isLoading = usingSearch ? false : !!tabLoading[activeTab]


  if (detailView || detailLoading) {
    return (
      <div className="flex flex-col h-full animate-fade-in">
        <button onClick={() => { setDetailView(null); onSelectOpp(null) }}
          className="flex items-center gap-1.5 text-xs text-slate-400 hover:text-slate-200
                     transition-colors mb-3 group">
          <ArrowLeft size={13} className="group-hover:-translate-x-0.5 transition-transform" />
          Back to list
        </button>

        {detailLoading ? (
          <div className="flex-1 flex items-center justify-center"><Spinner /></div>
        ) : (
          <div className="flex-1 overflow-y-auto space-y-3 pr-1">
            {}
            <div>
              <div className="flex items-center gap-2 mb-2">
                <SourceBadge source={detailView.source} />
                {detailView.sector && (
                  <span className="text-2xs text-brand-400 font-medium">{detailView.sector}</span>
                )}
              </div>
              <h2 className="text-base font-bold text-slate-100 leading-snug">
                {detailView.title}
              </h2>
            </div>

            {}
            <div className="grid grid-cols-2 gap-2">
              {[
                ['Agency',      detailView.agency],
                ['Max Funding', fmt(detailView.funding_max)],
                ['Min Funding', fmt(detailView.funding_min)],
                ['Closes',      detailView.close_date],
                ['Posted',      detailView.posted_date],
                ['Geography',   detailView.geography],
                ['Eligibility', detailView.eligibility],
                ['NAICS',       detailView.naics_code],
              ].filter(([, v]) => v).map(([label, val]) => (
                <div key={label} className="bg-white/[0.03] rounded-lg p-2.5">
                  <p className="text-2xs text-slate-500 mb-0.5">{label}</p>
                  <p className="text-xs font-semibold text-slate-200 break-words">{val}</p>
                </div>
              ))}
            </div>

            {}
            {detailView.description && (
              <div>
                <p className="section-label mb-1.5">Description</p>
                <p className="text-xs text-slate-400 leading-relaxed line-clamp-6">
                  {detailView.description}
                </p>
              </div>
            )}

            {}
            {detailView.tags?.length > 0 && (
              <div className="flex flex-wrap gap-1.5">
                {(typeof detailView.tags === 'string'
                  ? detailView.tags.split(',')
                  : detailView.tags
                ).filter(Boolean).map((t, i) => (
                  <span key={i}
                    className="text-2xs px-2 py-0.5 rounded-full bg-white/[0.05]
                               text-slate-400 border border-white/[0.06]">
                    {t.trim()}
                  </span>
                ))}
              </div>
            )}

            {}
            {detailView.source_url && (
              <a href={detailView.source_url} target="_blank" rel="noreferrer"
                className="flex items-center gap-1.5 text-xs text-brand-400 hover:underline">
                <ExternalLink size={12} /> View original source
              </a>
            )}
          </div>
        )}
      </div>
    )
  }


  return (
    <div className="flex flex-col h-full">
      {}
      {!usingSearch && (
        <div className="flex gap-1 mb-3 p-1 bg-white/[0.03] rounded-lg shrink-0">
          {TABS.map(({ key, label, icon: Icon, color }) => (
            <button key={key} onClick={() => setActiveTab(key)}
              className={`flex-1 flex items-center justify-center gap-1.5 py-1.5 rounded-md
                          text-xs font-medium transition-all
                          ${activeTab === key
                            ? 'bg-surface-800 text-slate-100 shadow-sm'
                            : 'text-slate-500 hover:text-slate-300'}`}>
              <Icon size={12} className={activeTab === key ? color : ''} />
              {label}
            </button>
          ))}
        </div>
      )}

      {}
      {usingSearch && searchQuery && (
        <div className="flex items-center gap-2 mb-3 px-1 shrink-0">
          <Sparkles size={12} className="text-brand-400" />
          <p className="text-xs text-slate-400">
            Results for <span className="text-slate-200 font-medium">"{searchQuery}"</span>
          </p>
        </div>
      )}

      {}
      <div className="flex-1 overflow-y-auto space-y-2 pr-1">
        {isLoading ? (
          <div className="flex items-center justify-center h-32"><Spinner /></div>
        ) : displayList.length === 0 ? (
          <EmptyState text="No opportunities found. Try a different search or refresh the data." />
        ) : (
          displayList.map(opp => (
            <OppCard key={opp.opp_id} opp={opp}
              onClick={handleCardClick}
              active={selectedOpp?.opp_id === opp.opp_id} />
          ))
        )}
      </div>

      {}
      {!usingSearch && total > 20 && (
        <div className="flex justify-between items-center mt-3 pt-3 border-t border-white/[0.06] shrink-0">
          <button disabled={offset === 0} onClick={() => loadTab(activeTab, offset - 20)}
            className="btn-ghost text-xs py-1.5 disabled:opacity-30">← Prev</button>
          <span className="text-2xs text-slate-500 tabular-nums">
            {offset + 1}–{Math.min(offset + 20, total)} of {total.toLocaleString()}
          </span>
          <button disabled={offset + 20 >= total} onClick={() => loadTab(activeTab, offset + 20)}
            className="btn-ghost text-xs py-1.5 disabled:opacity-30">Next →</button>
        </div>
      )}
    </div>
  )
}



function IntelPanel({ selectedOpp }) {
  const [briefing, setBriefing]         = useState(null)
  const [explanation, setExplanation]   = useState(null)
  const [briefingLoading, setBriefingLoading] = useState(false)
  const [explainLoading, setExplainLoading]   = useState(false)
  const prevSector = useRef(null)


  useEffect(() => {
    if (!selectedOpp) {
      setBriefing(null)
      setExplanation(null)
      return
    }

    setExplainLoading(true)
    explainOpportunity(selectedOpp.opp_id)
      .then(setExplanation)
      .catch(() => setExplanation(null))
      .finally(() => setExplainLoading(false))

    if (selectedOpp.sector && selectedOpp.sector !== prevSector.current) {
      prevSector.current = selectedOpp.sector
      setBriefingLoading(true)
      fetchSectorBriefing(selectedOpp.sector)
        .then(setBriefing)
        .catch(() => setBriefing(null))
        .finally(() => setBriefingLoading(false))
    }
  }, [selectedOpp?.opp_id])

  if (!selectedOpp) {
    return (
      <div className="flex flex-col gap-3 h-full">
        <div className="card flex-1 flex flex-col">
          <div className="flex items-center gap-2 px-4 pt-4 pb-3 border-b border-white/[0.06]">
            <TrendingUp size={14} className="text-brand-400" />
            <p className="text-sm font-semibold text-slate-200">Runway Planner</p>
          </div>
          <EmptyState text="Select an opportunity to see your runway analysis" />
        </div>
        <div className="card flex-1 flex flex-col">
          <div className="flex items-center gap-2 px-4 pt-4 pb-3 border-b border-white/[0.06]">
            <BarChart2 size={14} className="text-brand-400" />
            <p className="text-sm font-semibold text-slate-200">Sector Intelligence</p>
          </div>
          <EmptyState text="Select an opportunity to load sector analysis" />
        </div>
      </div>
    )
  }

  return (
    <div className="flex flex-col gap-3 h-full overflow-y-auto">

      {}
      <div className="card shrink-0">
        <div className="flex items-center gap-2 px-4 pt-4 pb-3 border-b border-white/[0.06]">
          <TrendingUp size={14} className="text-brand-400" />
          <p className="text-sm font-semibold text-slate-200">Opportunity Explainer</p>
        </div>
        <div className="p-4">
          {explainLoading ? (
            <div className="flex items-center gap-2 text-xs text-slate-500">
              <Spinner size={13} /> Analyzing with AI…
            </div>
          ) : explanation ? (
            <div className="text-xs text-slate-300 leading-relaxed whitespace-pre-wrap">
              {explanation.explanation}
            </div>
          ) : (
            <p className="text-xs text-slate-500">Could not load explanation.</p>
          )}
        </div>
      </div>

      {}
      <div className="card shrink-0">
        <div className="flex items-center justify-between px-4 pt-4 pb-3 border-b border-white/[0.06]">
          <div className="flex items-center gap-2">
            <BarChart2 size={14} className="text-brand-400" />
            <p className="text-sm font-semibold text-slate-200">Sector Intelligence</p>
          </div>
          {selectedOpp.sector && (
            <span className="text-2xs text-brand-400 font-medium">{selectedOpp.sector}</span>
          )}
        </div>
        <div className="p-4">
          {briefingLoading ? (
            <div className="flex items-center gap-2 text-xs text-slate-500">
              <Spinner size={13} /> Generating briefing…
            </div>
          ) : briefing ? (
            <div className="space-y-3">
              {}
              {briefing.data && (
                <div className="grid grid-cols-2 gap-2 mb-3">
                  {[
                    ['Validation', briefing.data.validation_grade],
                    ['Risk',       briefing.data.risk_level],
                    ['Contracts',  briefing.data.open_contracts],
                    ['Failures',   briefing.data.known_failures],
                  ].filter(([, v]) => v != null).map(([label, val]) => (
                    <div key={label} className="bg-white/[0.03] rounded-lg p-2">
                      <p className="text-2xs text-slate-500">{label}</p>
                      <p className={`text-xs font-bold mt-0.5 ${
                        val === 'HIGH'   ? 'text-danger-400' :
                        val === 'MEDIUM' ? 'text-warning-400' :
                        val === 'LOW'    ? 'text-success-400' : 'text-slate-200'
                      }`}>{val}</p>
                    </div>
                  ))}
                </div>
              )}
              {}
              <div className="text-xs text-slate-400 leading-relaxed line-clamp-12">
                {briefing.briefing}
              </div>
            </div>
          ) : (
            <p className="text-xs text-slate-500">No sector data available.</p>
          )}
        </div>
      </div>
    </div>
  )
}


export default function Home() {
  const [stats, setStats]           = useState(null)
  const [pipeline, setPipeline]     = useState(null)
  const [refreshing, setRefreshing] = useState(false)
  const [searchResults, setSearchResults] = useState(null)
  const [searchQuery, setSearchQuery]     = useState('')
  const [searchLoading, setSearchLoading] = useState(false)
  const [selectedOpp, setSelectedOpp]     = useState(null)


  useEffect(() => {
    fetchStats().then(setStats).catch(() => {})
    fetchPipelineStatus().then(setPipeline).catch(() => {})
  }, [])

  const handleSearch = async ({ q, minFunding, sector }) => {
    if (!q && !minFunding && !sector) {
      setSearchResults(null)
      setSearchQuery('')
      return
    }
    setSearchLoading(true)
    setSearchQuery(q)
    try {
      const data = await semanticSearch({ query: q || sector || '', limit: 50 })
      setSearchResults(data.results)
    } catch {
      setSearchResults([])
    } finally {
      setSearchLoading(false)
    }
  }

  const handleRefresh = async () => {
    setRefreshing(true)
    try {
      await triggerPipelineRun()
      setTimeout(async () => {
        const [s, p] = await Promise.all([fetchStats(), fetchPipelineStatus()])
        setStats(s)
        setPipeline(p)
        setRefreshing(false)
      }, 3000)
    } catch {
      setRefreshing(false)
    }
  }

  return (
    <div className="flex flex-col h-screen p-4 gap-3 min-w-0">

      {}
      <DirectoryPanel
        stats={stats}
        pipeline={pipeline}
        onRefresh={handleRefresh}
        refreshing={refreshing}
      />

      {}
      <SearchBar onSearch={handleSearch} loading={searchLoading} />

      {}
      <div className="flex gap-3 flex-1 min-h-0">

        {}
        <div className="card flex-1 min-w-0 p-4 flex flex-col">
          <OpportunityPanel
            searchResults={searchResults}
            searchQuery={searchQuery}
            onSelectOpp={setSelectedOpp}
            selectedOpp={selectedOpp}
          />
        </div>

        {}
        <div className="w-80 shrink-0 flex flex-col gap-3 min-h-0 overflow-y-auto">
          <IntelPanel selectedOpp={selectedOpp} />
        </div>

      </div>
    </div>
  )
}