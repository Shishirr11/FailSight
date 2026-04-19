import { useState, useEffect, useRef, useCallback } from 'react'
import {
  Search, SlidersHorizontal, X, RefreshCw, ChevronDown,
  Database, ArrowLeft, Sparkles, FileText, Layers, Cpu,
  Microscope, FlaskConical, CheckCircle, Zap, ChevronRight,
  ExternalLink
} from 'lucide-react'
import { semanticSearch } from '../api/search'
import { fetchOpportunities, fetchStats, fetchOpportunity } from '../api/opportunities'
import { triggerPipelineRun } from '../api/pipeline'
import { fetchSectorBriefing } from '../api/briefings'
import { whyCare } from '../api/groq'

const fmt = n =>
  !n || n === 0 ? null
  : n >= 1e9 ? `$${(n/1e9).toFixed(1)}B`
  : n >= 1e6 ? `$${(n/1e6).toFixed(1)}M`
  : n >= 1e3 ? `$${(n/1e3).toFixed(0)}K`
  : `$${Math.round(n)}`

const SOURCE_TABS = [
  { key: 'all',      label: 'All',      icon: Database,     color: 'text-grey-600'    },
  { key: 'grants',   label: 'Grants',   icon: FileText,     color: 'text-emerald-600' },
  { key: 'sam',      label: 'SAM',      icon: Layers,       color: 'text-blue-600'    },
  { key: 'patents',  label: 'Patents',  icon: Cpu,          color: 'text-purple-600'  },
  { key: 'research', label: 'Research', icon: Microscope,   color: 'text-amber-600'   },
  { key: 'sbir',     label: 'SBIR',     icon: FlaskConical, color: 'text-green-600'   },
]

const ALL_SECTORS = [
  'AI & Machine Learning', 'Cybersecurity', 'Clean Energy', 'Climate Technology',
  'Biotechnology', 'Health Technology', 'Quantum Computing', 'Advanced Manufacturing',
  'Aerospace & Defense', 'Agriculture Technology', 'Advanced Computing', 'Fintech',
  'Transportation', 'Infrastructure', 'Education', 'Small Business', 'Community Development',
]

function SourceBadge({ source }) {
  const cls = {
    grants: 'badge-grants', sam: 'badge-sam', patents: 'badge-patents',
    research: 'badge-research', sbir: 'badge-sbir', nsf: 'badge-nsf',
  }
  return (
    <span className={`${cls[source] ?? 'chip chip-grey'} text-xs font-bold px-2.5 py-1 rounded-full`}>
      {source?.toUpperCase()}
    </span>
  )
}

function Spinner({ size = 18, className = '' }) {
  return <RefreshCw size={size} className={`animate-spin text-grey-400 ${className}`} />
}

function buildSourceLinks(opp, detail) {
  const kf = detail?.key_fields || {}
  const links = []

  if (opp.source === 'grants') {
    if (kf.program_url) links.push({ label: 'Grants.gov listing', url: kf.program_url })
    else if (kf.opp_num) links.push({ label: 'Grants.gov listing', url: `https://grants.gov/search-grants?oppNum=${kf.opp_num}` })
  }

  if (opp.source === 'sam') {
    if (kf.ui_link) links.push({ label: 'SAM.gov listing', url: kf.ui_link })
    else links.push({ label: 'SAM.gov search', url: `https://sam.gov/search/?keywords=${encodeURIComponent(opp.title?.slice(0, 60) || '')}` })
  }

  if (opp.source === 'sbir') {
    if (kf.company_url) links.push({ label: 'Company website', url: kf.company_url })
    links.push({ label: 'SBIR.gov search', url: `https://www.sbir.gov/sbirsearch/detail/funding?firm=${encodeURIComponent(kf.firm || '')}` })
  }

  if (opp.source === 'research') {
    if (kf.doi) links.push({ label: 'DOI / paper', url: kf.doi.startsWith('http') ? kf.doi : `https://doi.org/${kf.doi}` })
    if (kf.oa_url && kf.oa_url !== kf.doi) links.push({ label: 'Open access PDF', url: kf.oa_url })
    if (kf.openalex_id) links.push({ label: 'OpenAlex record', url: kf.openalex_id.startsWith('http') ? kf.openalex_id : `https://openalex.org/${kf.openalex_id}` })
  }

  if (opp.source === 'patents') {
    if (kf.patent_id) links.push({ label: 'Google Patents', url: `https://patents.google.com/patent/${kf.patent_id}` })
  }

  if (opp.source === 'nsf') {
    if (kf.nsf_id) links.push({ label: 'NSF Award page', url: `https://www.nsf.gov/awardsearch/showAward?AWD_ID=${kf.nsf_id}` })
  }

  return links
}

async function fetchSectorSuggestions(input) {
  if (!input || input.length < 3) return []
  try {
    const res = await fetch('/api/groq/sector-suggest', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ input }),
    })
    if (res.ok) {
      const data = await res.json()
      return data.sectors || []
    }
  } catch {}
  return ALL_SECTORS.filter(s =>
    s.toLowerCase().includes(input.toLowerCase())
  ).slice(0, 5)
}

function FilterModal({ filters, onApply, onClose }) {
  const [local, setLocal]           = useState(filters)
  const [sectorInput, setSectorInput] = useState(filters.sector || '')
  const [suggestions, setSuggestions] = useState([])
  const [loadingSuggest, setLoadingSuggest] = useState(false)
  const suggestTimer = useRef(null)

  const set = (k, v) => setLocal(p => ({ ...p, [k]: v }))

  const handleSectorChange = val => {
    setSectorInput(val)
    set('sector', val)
    clearTimeout(suggestTimer.current)
    if (val.length >= 2) {
      setLoadingSuggest(true)
      suggestTimer.current = setTimeout(async () => {
        const s = await fetchSectorSuggestions(val)
        setSuggestions(s)
        setLoadingSuggest(false)
      }, 350)
    } else {
      setSuggestions([])
      setLoadingSuggest(false)
    }
  }

  const pickSector = s => {
    setSectorInput(s)
    set('sector', s)
    setSuggestions([])
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/20"
         onClick={e => e.target === e.currentTarget && onClose()}>
      <div className="bg-white rounded-2xl shadow-lg border border-grey-200 w-[460px] animate-pop-in">
        <div className="flex items-center justify-between px-6 py-5 border-b border-grey-100">
          <p className="font-bold text-grey-900 text-lg">Filter results</p>
          <button onClick={onClose} className="btn-ghost p-2"><X size={16} /></button>
        </div>
        <div className="px-6 py-5 space-y-5">

          <div>
            <div className="flex items-center justify-between mb-2">
              <p className="section-label">Sector</p>
              {loadingSuggest && <Spinner size={12} />}
            </div>
            <div className="relative">
              <input className="input text-base" value={sectorInput}
                onChange={e => handleSectorChange(e.target.value)}
                placeholder="Type to get AI-matched sectors…" />
              {suggestions.length > 0 && (
                <div className="absolute top-full left-0 right-0 mt-1 bg-white border border-grey-200
                                rounded-xl shadow-md z-10 overflow-hidden animate-fade-in">
                  {suggestions.map(s => (
                    <button key={s} onClick={() => pickSector(s)}
                      className="w-full text-left px-4 py-2.5 text-sm font-medium text-grey-700
                                 hover:bg-green-50 hover:text-green-700 transition-colors flex items-center gap-2">
                      <Sparkles size={12} className="text-green-500 shrink-0" />
                      {s}
                    </button>
                  ))}
                </div>
              )}
            </div>

            <div className="flex flex-wrap gap-1.5 mt-2.5">
              {['AI & Machine Learning', 'Biotechnology', 'Clean Energy', 'Cybersecurity', 'Health Technology'].map(s => (
                <button key={s} onClick={() => pickSector(s)}
                  className={`chip text-xs font-semibold transition-colors
                    ${local.sector === s ? 'chip-green' : 'chip-grey hover:border-grey-300'}`}>
                  {s}
                </button>
              ))}
            </div>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <p className="section-label mb-2">Min funding</p>
              <input className="input text-base" type="number" value={local.minFunding || ''}
                onChange={e => set('minFunding', e.target.value ? Number(e.target.value) : null)}
                placeholder="e.g. 50000" />
            </div>
            <div>
              <p className="section-label mb-2">Max funding</p>
              <input className="input text-base" type="number" value={local.maxFunding || ''}
                onChange={e => set('maxFunding', e.target.value ? Number(e.target.value) : null)}
                placeholder="e.g. 500000" />
            </div>
          </div>

          <div>
            <p className="section-label mb-2">Agency</p>
            <input className="input text-base" value={local.agency || ''}
              onChange={e => set('agency', e.target.value)}
              placeholder="e.g. NIH, NSF, DOE" />
          </div>

          <label className="flex items-center gap-3 cursor-pointer">
            <input type="checkbox" checked={!!local.openOnly}
              onChange={e => set('openOnly', e.target.checked)}
              className="rounded border-grey-300 text-green-600 focus:ring-green-400 w-4 h-4" />
            <span className="text-base text-grey-700">Open opportunities only</span>
          </label>
        </div>
        <div className="flex gap-2 px-6 py-4 border-t border-grey-100">
          <button onClick={() => { setLocal({}); setSectorInput(''); setSuggestions([]); onApply({}) }}
            className="btn-ghost text-sm">Remove all filters</button>
          <div className="flex-1" />
          <button onClick={onClose} className="btn-secondary text-sm">Cancel</button>
          <button onClick={() => { onApply(local); onClose() }} className="btn-primary text-sm">Apply filters</button>
        </div>
      </div>
    </div>
  )
}

function Toast({ message, onDone }) {
  useEffect(() => { const t = setTimeout(onDone, 4000); return () => clearTimeout(t) }, [onDone])
  return (
    <div className="fixed bottom-6 right-6 z-50 flex items-center gap-3 bg-white border border-green-200
                    rounded-xl px-5 py-3.5 shadow-lg animate-slide-up">
      <CheckCircle size={18} className="text-green-600 shrink-0" />
      <p className="text-base text-grey-800">{message}</p>
    </div>
  )
}

function LegendDropdown({ stats, onRefresh, refreshing }) {
  const [open, setOpen] = useState(false)
  const ref = useRef(null)
  useEffect(() => {
    const h = e => { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    document.addEventListener('mousedown', h)
    return () => document.removeEventListener('mousedown', h)
  }, [])
  const total = stats?.total || 0
  return (
    <div className="relative" ref={ref}>
      <button onClick={() => setOpen(o => !o)}
        className="flex items-center gap-2 btn-secondary text-sm h-10 px-4">
        <Database size={15} className="text-grey-500" />
        <span className="text-grey-700 font-bold tabular-nums">{total.toLocaleString()}</span>
        <span className="text-grey-400">records</span>
        <ChevronDown size={13} className={`text-grey-400 transition-transform ${open ? 'rotate-180' : ''}`} />
      </button>
      {open && (
        <div className="absolute right-0 top-full mt-2 w-72 bg-white border border-grey-200
                        rounded-xl shadow-lg z-30 p-4 animate-fade-in">
          <p className="section-label mb-3">Data sources</p>
          <div className="space-y-2.5">
            {stats?.by_source?.map(s => (
              <div key={s.source} className="flex items-center justify-between">
                <SourceBadge source={s.source} />
                <span className="text-sm font-bold text-grey-600 tabular-nums">{s.count?.toLocaleString()}</span>
              </div>
            ))}
          </div>
          <div className="mt-4 pt-4 border-t border-grey-100">
            <button onClick={() => { onRefresh(); setOpen(false) }} disabled={refreshing}
              className="w-full btn-primary text-sm py-2.5 flex items-center justify-center gap-2">
              <RefreshCw size={14} className={refreshing ? 'animate-spin' : ''} />
              {refreshing ? 'Fetching…' : 'Fetch new records'}
            </button>
          </div>
        </div>
      )}
    </div>
  )
}

function OppCard({ opp, selected, onSelect, onExpand }) {
  const isExpiring = opp.close_date &&
    new Date(opp.close_date) < new Date(Date.now() + 14 * 86400000) &&
    new Date(opp.close_date) > new Date()

  return (
    <div onClick={() => onSelect(opp)}
      className={`relative w-full text-left p-4 rounded-xl border transition-all duration-150 cursor-pointer group
        ${selected ? 'bg-green-50 border-green-300 shadow-sm' : 'bg-white border-grey-200 hover:border-grey-300 hover:shadow-sm'}`}>

      <div className="flex items-center gap-2 mb-3">
        <SourceBadge source={opp.source} />
        {isExpiring && (
          <span className="text-xs px-2.5 py-1 rounded-full bg-red-50 text-red-600 border border-red-100 font-bold">
            Closing soon
          </span>
        )}
      </div>

      <p className="text-base font-bold text-grey-900 leading-snug line-clamp-2 mb-1.5 pr-16">
        {opp.title}
      </p>
      <p className="text-sm text-grey-500 mb-3 line-clamp-1 font-medium">{opp.agency}</p>

      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2 flex-wrap min-w-0">
          {opp.sector && (
            <span className="text-xs px-2.5 py-1 rounded-full bg-grey-100 text-grey-600 border border-grey-200 font-semibold">
              {opp.sector}
            </span>
          )}
          {opp.relevance_pct != null && (
            <span className="text-xs text-grey-400 font-semibold">{opp.relevance_pct}% match</span>
          )}
        </div>
        <div className="flex items-center gap-2.5 shrink-0">
          {fmt(opp.funding_max) && (
            <span className="text-sm font-bold text-green-700">{fmt(opp.funding_max)}</span>
          )}
          <button
            onClick={e => { e.stopPropagation(); onExpand(opp) }}
            className={`flex items-center gap-1 text-xs font-bold px-2.5 py-1.5 rounded-lg border transition-all
              ${selected
                ? 'bg-green-100 border-green-300 text-green-700'
                : 'bg-grey-50 border-grey-200 text-grey-600 hover:bg-grey-100 opacity-0 group-hover:opacity-100'
              }`}
          >
            View <ChevronRight size={11} />
          </button>
        </div>
      </div>
    </div>
  )
}

function OppDetail({ opp, onBack, searchQuery }) {
  const [whyCareText, setWhyCareText] = useState(null)
  const [loadingWhy, setLoadingWhy]   = useState(false)
  const [detail, setDetail]           = useState(null)

  useEffect(() => {
    if (!opp) return
    setDetail(null)
    fetchOpportunity(opp.opp_id)
      .then(setDetail)
      .catch(() => setDetail(null))
  }, [opp?.opp_id])

  useEffect(() => {
    if (!opp || !searchQuery) return
    setLoadingWhy(true)
    setWhyCareText(null)
    whyCare(opp.opp_id, searchQuery)
      .then(r => setWhyCareText(r.explanation))
      .catch(() => setWhyCareText(null))
      .finally(() => setLoadingWhy(false))
  }, [opp?.opp_id, searchQuery])

  if (!opp) return null

  const sourceLinks = buildSourceLinks(opp, detail)
  const kf = detail?.key_fields || {}

  return (
    <div className="flex flex-col h-full bg-white">
      <div className="flex items-center gap-3 px-5 py-4 border-b border-grey-100 shrink-0">
        <button onClick={onBack}
          className="flex items-center gap-1.5 text-sm font-bold text-grey-500
                     hover:text-grey-800 hover:bg-grey-100 px-3 py-2 rounded-lg transition-colors">
          <ArrowLeft size={15} /> Back
        </button>
        <span className="text-sm text-grey-400 truncate flex-1 font-medium">{opp.title}</span>
      </div>

      <div className="flex-1 overflow-y-auto p-6 space-y-6">
        <div className="flex flex-wrap items-center gap-2">
          <SourceBadge source={opp.source} />
          {opp.sector && <span className="chip chip-grey text-sm font-semibold">{opp.sector}</span>}
        </div>

        <div>
          <h2 className="text-2xl font-bold text-grey-900 leading-snug mb-2">{opp.title}</h2>
          {opp.agency && <p className="text-base text-grey-500 font-medium">{opp.agency}</p>}
        </div>

        {sourceLinks.length > 0 && (
          <div className="flex flex-wrap gap-2">
            {sourceLinks.map((link, i) => (
              <a key={i} href={link.url} target="_blank" rel="noopener noreferrer"
                className="flex items-center gap-1.5 text-sm font-semibold text-green-700
                           bg-green-50 border border-green-200 hover:bg-green-100
                           px-3 py-1.5 rounded-lg transition-colors">
                <ExternalLink size={13} />
                {link.label}
              </a>
            ))}
          </div>
        )}

        {searchQuery && (
          <div className="bg-green-50 border border-green-200 rounded-xl p-5">
            <div className="flex items-center gap-2 mb-2.5">
              <Sparkles size={15} className="text-green-600" />
              <p className="text-sm font-bold text-green-700 uppercase tracking-wider">Why this matters</p>
            </div>
            {loadingWhy ? (
              <div className="flex items-center gap-2">
                <Spinner size={14} /><span className="text-base text-grey-400">Analysing relevance…</span>
              </div>
            ) : (
              <p className="text-base text-green-900 leading-relaxed">{whyCareText || '—'}</p>
            )}
          </div>
        )}

        <div className="grid grid-cols-2 gap-3">
          {fmt(opp.funding_max) && (
            <div className="bg-grey-50 rounded-xl p-4 border border-grey-100">
              <p className="section-label mb-2">Max funding</p>
              <p className="text-xl font-bold text-green-700">{fmt(opp.funding_max)}</p>
            </div>
          )}
          {fmt(opp.funding_min) && (
            <div className="bg-grey-50 rounded-xl p-4 border border-grey-100">
              <p className="section-label mb-2">Min funding</p>
              <p className="text-xl font-bold text-grey-700">{fmt(opp.funding_min)}</p>
            </div>
          )}
          {opp.close_date && (
            <div className="bg-grey-50 rounded-xl p-4 border border-grey-100">
              <p className="section-label mb-2">Closes</p>
              <p className="text-base font-bold text-grey-800">{opp.close_date}</p>
            </div>
          )}
          {opp.posted_date && (
            <div className="bg-grey-50 rounded-xl p-4 border border-grey-100">
              <p className="section-label mb-2">Posted</p>
              <p className="text-base font-bold text-grey-800">{opp.posted_date}</p>
            </div>
          )}
        </div>

        {(opp.source === 'research') && (kf.authors || kf.cited_by != null) && (
          <div className="space-y-3">
            {kf.authors && (
              <div>
                <p className="section-label mb-1.5">Authors</p>
                <p className="text-sm text-grey-700">{kf.authors}</p>
              </div>
            )}
            {kf.cited_by != null && (
              <div className="inline-flex items-center gap-2 bg-purple-50 border border-purple-100 rounded-lg px-3 py-2">
                <p className="text-xs font-bold text-purple-600 uppercase tracking-wide">Citations</p>
                <p className="text-base font-bold text-purple-700">{kf.cited_by}</p>
              </div>
            )}
          </div>
        )}

        {(opp.source === 'patents') && (kf.assignees || kf.inventors) && (
          <div className="space-y-3">
            {kf.assignees && (
              <div>
                <p className="section-label mb-1.5">Assignees</p>
                <p className="text-sm text-grey-700">{kf.assignees}</p>
              </div>
            )}
            {kf.inventors && (
              <div>
                <p className="section-label mb-1.5">Inventors</p>
                <p className="text-sm text-grey-700">{Array.isArray(kf.inventors) ? kf.inventors.join(', ') : kf.inventors}</p>
              </div>
            )}
          </div>
        )}

        {(opp.source === 'sbir' || opp.source === 'nsf') && kf.pi_name && (
          <div>
            <p className="section-label mb-1.5">Principal Investigator</p>
            <p className="text-sm font-semibold text-grey-800">{kf.pi_name}</p>
            {kf.pi_email && <p className="text-sm text-grey-500 mt-0.5">{kf.pi_email}</p>}
          </div>
        )}

        {opp.source === 'grants' && kf.grantor_contact && (
          <div>
            <p className="section-label mb-1.5">Grantor contact</p>
            <p className="text-sm font-semibold text-grey-800">{kf.grantor_contact}</p>
            {kf.grantor_email && (
              <a href={`mailto:${kf.grantor_email}`} className="text-sm text-green-600 hover:underline">
                {kf.grantor_email}
              </a>
            )}
          </div>
        )}

        {opp.source === 'sam' && kf.solicitation_number && (
          <div>
            <p className="section-label mb-1.5">Solicitation number</p>
            <p className="text-sm font-mono font-semibold text-grey-800">{kf.solicitation_number}</p>
          </div>
        )}

        {opp.eligibility && (
          <div>
            <p className="section-label mb-2">Eligibility</p>
            <p className="text-base text-grey-700 leading-relaxed">{opp.eligibility}</p>
          </div>
        )}

        {opp.description && (
          <div>
            <p className="section-label mb-2">Description</p>
            <p className="text-base text-grey-700 leading-relaxed line-clamp-8">{opp.description}</p>
          </div>
        )}

        {opp.tags && (
          <div>
            <p className="section-label mb-2">Tags</p>
            <div className="flex flex-wrap gap-2">
              {opp.tags.split(',').filter(Boolean).map(t => (
                <span key={t} className="chip chip-grey text-sm font-semibold">{t.trim()}</span>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

function SectorIntelligence({ sector }) {
  const [briefing, setBriefing] = useState(null)
  const [loading, setLoading]   = useState(false)
  const [error, setError]       = useState(null)

  useEffect(() => {
    if (!sector) { setBriefing(null); return }
    setLoading(true); setError(null); setBriefing(null)
    fetchSectorBriefing(sector)
      .then(setBriefing)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [sector])

  if (!sector) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-center px-8">
        {/* <img src="/brain-logo.png" alt="" className="w-14 h-14 object-contain opacity-30 mb-4" /> */}
        <p className="text-lg font-bold text-grey-500">Select a record</p>
        <p className="text-base text-grey-400 mt-2 leading-relaxed">
          Click any opportunity on the left to see sector intelligence here
        </p>
      </div>
    )
  }

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-3">
        <Spinner size={22} /><p className="text-base text-grey-400">Loading sector intelligence…</p>
      </div>
    )
  }

  if (error) {
    return <div className="p-5 text-base text-red-600 bg-red-50 rounded-xl m-5 border border-red-100">{error}</div>
  }

  if (!briefing) return null

  const { validation, risk, white_space, competitors, grants = [], failures = [] } = briefing

  const riskColor = {
    LOW:     'text-green-700 bg-green-50 border-green-200',
    MEDIUM:  'text-amber-700 bg-amber-50 border-amber-200',
    HIGH:    'text-red-700 bg-red-50 border-red-200',
    UNKNOWN: 'text-grey-600 bg-grey-100 border-grey-200',
  }[risk?.risk_level] || 'text-grey-600 bg-grey-100 border-grey-200'

  const gradeColor = {
    A: 'text-green-700 bg-green-100', B: 'text-blue-700 bg-blue-100',
    C: 'text-amber-700 bg-amber-100', D: 'text-red-700 bg-red-100',
  }[validation?.grade] || 'text-grey-600 bg-grey-100'

  return (
    <div className="flex flex-col h-full overflow-y-auto p-5 space-y-5">
      <div className="flex items-start justify-between gap-2">
        <div>
          <p className="section-label mb-1.5">Sector intelligence</p>
          <h3 className="text-xl font-bold text-grey-900">{sector}</h3>
        </div>
        <div className={`w-12 h-12 rounded-xl flex items-center justify-center text-lg font-bold ${gradeColor}`}>
          {validation?.grade || '?'}
        </div>
      </div>

      {validation && (
        <div className="bg-grey-50 rounded-xl p-4 border border-grey-100">
          <div className="flex items-center justify-between mb-3">
            <p className="section-label">Market score</p>
            <span className="text-base font-bold text-grey-800">{validation.score}/100</span>
          </div>
          <div className="h-2.5 bg-grey-200 rounded-full overflow-hidden">
            <div className="h-2.5 rounded-full bg-green-500 transition-all duration-700"
              style={{ width: `${validation.score}%` }} />
          </div>
          <p className="text-sm text-grey-500 mt-2.5 font-medium">{validation.label}</p>
        </div>
      )}

      {risk && (
        <div className={`rounded-xl p-4 border ${riskColor}`}>
          <div className="flex items-center justify-between mb-2">
            <p className="text-xs font-bold uppercase tracking-wider opacity-70">Risk level</p>
            <span className="text-base font-bold">{risk.risk_level}</span>
          </div>
          <p className="text-sm leading-relaxed opacity-80">{risk.recommendation}</p>
        </div>
      )}

      {validation?.signals && (
        <div>
          <p className="section-label mb-3">Signal breakdown</p>
          <div className="grid grid-cols-2 gap-2.5">
            {[
              ['Contracts', validation.signals.contracts,      'text-blue-700'],
              ['Grants',    validation.signals.grants,         'text-green-700'],
              ['Patents',   validation.signals.patents,        'text-purple-700'],
              ['Research',  validation.signals.research,       'text-amber-700'],
              ['Failures',  validation.signals.known_failures, 'text-red-700'],
            ].map(([label, val, color]) => (
              <div key={label} className="bg-white rounded-lg p-3 border border-grey-100">
                <p className="text-xs text-grey-400 mb-1 font-semibold">{label}</p>
                <p className={`text-2xl font-bold tabular-nums ${color}`}>{val}</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {white_space && (
        <div className="bg-blue-50 border border-blue-100 rounded-xl p-4">
          <p className="section-label mb-2 text-blue-600">White space</p>
          <div className="flex items-start justify-between gap-2">
            <p className="text-sm text-blue-800 leading-relaxed flex-1">{white_space.interpretation}</p>
            <span className={`text-sm font-bold ml-2 shrink-0 ${
              white_space.opportunity_level === 'HIGH' ? 'text-green-700'
              : white_space.opportunity_level === 'MODERATE' ? 'text-amber-700'
              : 'text-grey-600'
            }`}>{white_space.opportunity_level}</span>
          </div>
        </div>
      )}

      {competitors?.top_buyers?.length > 0 && (
        <div>
          <p className="section-label mb-3">Top buyers</p>
          <div className="space-y-2">
            {competitors.top_buyers.slice(0, 4).map((b, i) => (
              <div key={i} className="flex items-center justify-between bg-white rounded-lg px-4 py-3 border border-grey-100">
                <p className="text-sm font-semibold text-grey-700 truncate flex-1">{b.agency}</p>
                <span className="text-xs font-bold text-grey-500 ml-2 shrink-0">{b.contract_count} contracts</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {risk?.top_reasons?.length > 0 && (
        <div>
          <p className="section-label mb-3">Common failure reasons</p>
          <div className="flex flex-wrap gap-2">
            {risk.top_reasons.map(r => (
              <span key={r.reason} className="chip chip-red text-sm font-semibold">
                {r.reason.replace(/_/g, ' ')} ({r.count})
              </span>
            ))}
          </div>
        </div>
      )}

      {grants.length > 0 && (
        <div>
          <p className="section-label mb-3">Open grants</p>
          <div className="space-y-2">
            {grants.slice(0, 3).map((g, i) => (
              <div key={i} className="bg-white rounded-lg px-4 py-3 border border-grey-100">
                <p className="text-sm font-bold text-grey-800 line-clamp-1 mb-1">{g.title}</p>
                <div className="flex items-center justify-between">
                  <p className="text-xs text-grey-400 font-medium">{g.agency}</p>
                  {fmt(g.funding_max) && <span className="text-sm font-bold text-green-700">{fmt(g.funding_max)}</span>}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {failures.length > 0 && (
        <div>
          <p className="section-label mb-3">Known failures</p>
          <div className="space-y-2">
            {failures.slice(0, 3).map((f, i) => (
              <div key={i} className="bg-white rounded-lg px-4 py-3 border border-grey-100">
                <div className="flex items-center justify-between mb-0.5">
                  <p className="text-sm font-bold text-grey-800">{f.company_name}</p>
                  <span className="text-xs text-grey-400 font-medium">{f.year_failed}</span>
                </div>
                {f.key_lesson && <p className="text-xs text-grey-500 italic line-clamp-1">{f.key_lesson}</p>}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

export default function Home() {
  const [query, setQuery]               = useState('')
  const [activeSource, setActiveSource] = useState('all')
  const [filters, setFilters]           = useState({ openOnly: false })
  const [showFilters, setShowFilters]   = useState(false)
  const [results, setResults]           = useState([])
  const [total, setTotal]               = useState(0)
  const [loading, setLoading]           = useState(false)
  const [offset, setOffset]             = useState(0)
  const [selectedOpp, setSelectedOpp]   = useState(null)
  const [detailOpp, setDetailOpp]       = useState(null)
  const [lastQuery, setLastQuery]       = useState('')
  const [stats, setStats]               = useState(null)
  const [refreshing, setRefreshing]     = useState(false)
  const [toast, setToast]               = useState(null)
  const [leftWidth, setLeftWidth]       = useState(50)

  const containerRef = useRef(null)
  const dragging     = useRef(false)
  const debounceRef  = useRef(null)
  const LIMIT = 30

  const handleMouseDown = () => { dragging.current = true }
  const handleMouseMove = useCallback(e => {
    if (!dragging.current || !containerRef.current) return
    const rect = containerRef.current.getBoundingClientRect()
    setLeftWidth(Math.min(75, Math.max(25, ((e.clientX - rect.left) / rect.width) * 100)))
  }, [])
  const handleMouseUp = () => { dragging.current = false }

  useEffect(() => {
    window.addEventListener('mousemove', handleMouseMove)
    window.addEventListener('mouseup', handleMouseUp)
    return () => {
      window.removeEventListener('mousemove', handleMouseMove)
      window.removeEventListener('mouseup', handleMouseUp)
    }
  }, [handleMouseMove])

  useEffect(() => { fetchStats().then(setStats).catch(() => {}) }, [])

  const load = useCallback(async (q, src, flt, off = 0) => {
    setLoading(true)
    try {
      const sources = src === 'all' ? [] : [src]
      const trimmed = (q || '').trim()
      const sector  = flt?.sector?.trim() || ''

      const semanticQuery = trimmed
        ? (sector ? `${trimmed} ${sector}` : trimmed)
        : sector

    let data
      if (semanticQuery) {
        try {
          const controller = new AbortController()
          const timeout = setTimeout(() => controller.abort(), 25000)
          data = await semanticSearch({ query: semanticQuery, sources, limit: LIMIT, offset: off })
          clearTimeout(timeout)
        } catch (err) {
          console.warn('Semantic search failed, falling back to SQL:', err.message)
          data = await fetchOpportunities({
            source: sources, sector: filters?.sector || '',
            minFunding: flt?.minFunding || null,
            maxFunding: flt?.maxFunding || null,
            openOnly: flt?.openOnly !== false,
            limit: LIMIT, offset: off,
          })
        }
        if (flt?.minFunding || flt?.maxFunding) {
          const min = flt.minFunding || 0
          const max = flt.maxFunding || Infinity
          data.results = (data.results || []).filter(r => {
            const f = r.funding_max || r.funding_min || 0
            return f >= min && f <= max
          })
        }
      } else {
        data = await fetchOpportunities({
          source: sources,
          minFunding: flt?.minFunding || null,
          maxFunding: flt?.maxFunding || null,
          openOnly: flt?.openOnly !== false,
          limit: LIMIT,
          offset: off,
        })
      }

      setResults(data.results || [])
      setTotal(data.total || 0)
    } catch (e) { console.error(e) }
    finally { setLoading(false) }
  }, [])

  useEffect(() => { load('', 'all', { openOnly: true }) }, [load])

  useEffect(() => {
    clearTimeout(debounceRef.current)
    if (!query.trim()) {
      debounceRef.current = setTimeout(() => { setOffset(0); load('', activeSource, filters, 0) }, 300)
    }
  }, [query]) 

  const handleSearch = () => {
    setLastQuery(query); setOffset(0); setDetailOpp(null)
    load(query, activeSource, filters, 0)
  }

  const handleSourceChange = src => {
    setActiveSource(src); setOffset(0); load(query, src, filters, 0)
  }

  const handleApplyFilters = flt => {
    setFilters(flt); setOffset(0); load(query, activeSource, flt, 0)
  }

  const handleRefresh = async () => {
    setRefreshing(true)
    const prevTotal = stats?.total || 0
    try {
      await triggerPipelineRun(null)
      let newStats = stats
      for (let i = 0; i < 6; i++) {
        await new Promise(r => setTimeout(r, 5000))
        newStats = await fetchStats()
        if ((newStats.total || 0) !== prevTotal) break
      }
      const added = (newStats.total || 0) - prevTotal
      setStats({ ...newStats })
      setToast(`Refresh complete — ${added > 0 ? `+${added} new records added` : 'no new records found'}`)
      load(query, activeSource, filters, 0)
    } catch {
      setToast('Refresh triggered — check pipeline status for details')
      const latest = await fetchStats().catch(() => stats)
      if (latest) setStats({ ...latest })
    } finally { setRefreshing(false) }
  }

  const activeFiltersCount = Object.entries(filters).filter(([k, v]) =>
    k !== 'openOnly' && v !== null && v !== undefined && v !== ''
  ).length

  return (
    <div className="flex flex-col h-screen overflow-hidden bg-grey-50">

      <div className="bg-white border-b border-grey-200 px-5 py-3.5 flex items-center gap-4 shrink-0">
        <div className="flex items-center gap-2.5 shrink-0">
          {/* <img src="/brain-logo.png" alt="Findout" className="w-7 h-7 object-contain" /> */}
          <span className="font-display text-3.5xl text-base font-bold text-grey-800 italic">Findout</span>
        </div>
        <div className="w-px h-6 bg-grey-200" />
        <div className="flex-1 flex items-center gap-3 max-w-2xl">
          <div className="relative flex-1">
            <Search size={16} className="absolute left-3.5 top-1/2 -translate-y-1/2 text-grey-400 pointer-events-none" />
            <input className="input pl-10 pr-4 py-2.5 text-base h-11"
              value={query} onChange={e => setQuery(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleSearch()}
              placeholder="Search grants, contracts, research…" />
          </div>
          <button onClick={() => setShowFilters(true)}
            className={`btn-secondary h-11 px-4 flex items-center gap-2 text-sm font-semibold relative
              ${activeFiltersCount > 0 ? 'border-green-300 text-green-700' : ''}`}>
            <SlidersHorizontal size={15} />
            Filters
            {activeFiltersCount > 0 && (
              <span className="absolute -top-1.5 -right-1.5 w-5 h-5 bg-green-600 text-white
                               text-xs rounded-full flex items-center justify-center font-bold">
                {activeFiltersCount}
              </span>
            )}
          </button>
          <button onClick={handleSearch} disabled={loading}
            className="btn-primary h-11 px-5 text-sm font-semibold flex items-center gap-2">
            {loading ? <Spinner size={15} className="!text-white" /> : <Zap size={15} />}
            Search
          </button>
        </div>
        {filters.sector && (
          <span className="chip chip-green text-sm font-semibold flex items-center gap-1.5">
            {filters.sector}
            <button onClick={() => handleApplyFilters({ ...filters, sector: '' })}><X size={10} /></button>
          </span>
        )}
        <div className="flex-1" />
        <LegendDropdown stats={stats} onRefresh={handleRefresh} refreshing={refreshing} />
      </div>

      <div className="bg-white border-b border-grey-200 px-5 flex items-center gap-1 shrink-0">
        {SOURCE_TABS.map(({ key, label, icon: Icon, color }) => (
          <button key={key} onClick={() => handleSourceChange(key)}
            className={`flex items-center gap-2 px-4 py-3.5 text-sm font-bold border-b-2 transition-colors
              ${activeSource === key
                ? 'border-green-500 text-green-700'
                : 'border-transparent text-grey-500 hover:text-grey-700 hover:border-grey-200'
              }`}>
            <Icon size={15} className={activeSource === key ? 'text-green-600' : color} />
            {label}
          </button>
        ))}
        <div className="ml-auto pb-1">
          <span className="text-sm text-grey-400 font-semibold tabular-nums">{total.toLocaleString()} results</span>
        </div>
      </div>

      <div className="flex flex-1 overflow-hidden" ref={containerRef}>
        <div className="flex flex-col border-r border-grey-200 overflow-hidden bg-grey-50"
             style={{ width: `${leftWidth}%` }}>
          {detailOpp ? (
            <div className="flex-1 overflow-hidden">
              <OppDetail opp={detailOpp} onBack={() => setDetailOpp(null)} searchQuery={lastQuery} />
            </div>
          ) : (
            <>
              <div className="flex-1 overflow-y-auto p-4 space-y-3">
                {loading && results.length === 0 ? (
                  <div className="flex items-center justify-center h-40"><Spinner size={22} /></div>
                ) : results.length === 0 ? (
                  <div className="flex flex-col items-center justify-center h-40 text-center">
                    <p className="text-base font-semibold text-grey-500">No results found</p>
                    <p className="text-sm text-grey-400 mt-1">Try a different search or adjust filters</p>
                  </div>
                ) : (
                  results.map(opp => (
                    <OppCard key={opp.opp_id} opp={opp}
                      selected={selectedOpp?.opp_id === opp.opp_id}
                      onSelect={o => setSelectedOpp(o)}
                      onExpand={o => { setDetailOpp(o); setSelectedOpp(o) }}
                    />
                  ))
                )}
              </div>
              {total > LIMIT && (
                <div className="flex items-center justify-between px-4 py-3 border-t border-grey-100 shrink-0 bg-white">
                  <button disabled={offset === 0}
                    onClick={() => { const o = Math.max(0, offset - LIMIT); setOffset(o); load(query, activeSource, filters, o) }}
                    className="btn-secondary text-sm font-semibold disabled:opacity-40">← Prev</button>
                  <span className="text-sm text-grey-400 font-semibold">
                    {offset + 1}–{Math.min(offset + LIMIT, total)} of {total.toLocaleString()}
                  </span>
                  <button disabled={offset + LIMIT >= total}
                    onClick={() => { const o = offset + LIMIT; setOffset(o); load(query, activeSource, filters, o) }}
                    className="btn-secondary text-sm font-semibold disabled:opacity-40">Next →</button>
                </div>
              )}
            </>
          )}
        </div>

        <div onMouseDown={handleMouseDown}
          className="w-1.5 cursor-col-resize hover:bg-green-400 bg-grey-200 transition-colors shrink-0" />

        <div className="flex-1 overflow-hidden bg-white">
          <SectorIntelligence sector={selectedOpp?.sector || null} />
        </div>
      </div>

      {showFilters && (
        <FilterModal filters={filters} onApply={handleApplyFilters} onClose={() => setShowFilters(false)} />
      )}
      {toast && <Toast message={toast} onDone={() => setToast(null)} />}
    </div>
  )
}