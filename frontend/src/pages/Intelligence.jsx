import { useState, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { Search, ChevronDown, ChevronUp, Sparkles, AlertTriangle, RefreshCw, TrendingUp, Zap, ExternalLink } from 'lucide-react'
import { validateIdea, gapFinder, grantMatch } from '../api/groq'

const fmt = n =>
  !n || n === 0 ? null
  : n >= 1e6 ? `$${(n/1e6).toFixed(1)}M`
  : n >= 1e3 ? `$${(n/1e3).toFixed(0)}K`
  : `$${Math.round(n)}`

function Spinner({ size = 18 }) {
  return <RefreshCw size={size} className="animate-spin text-grey-400" />
}

function Section({ title, icon: Icon, children, defaultOpen = true }) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div className="bg-white border border-grey-200 rounded-2xl overflow-hidden shadow-sm">
      <button onClick={() => setOpen(o => !o)}
        className="w-full flex items-center justify-between px-6 py-5 hover:bg-grey-50 transition-colors">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-navy-50 flex items-center justify-center">
            <Icon size={16} className="text-navy-800" />
          </div>
          <span className="text-base font-bold text-grey-800">{title}</span>
        </div>
        {open ? <ChevronUp size={16} className="text-grey-400" /> : <ChevronDown size={16} className="text-grey-400" />}
      </button>
      {open && <div className="border-t border-grey-100">{children}</div>}
    </div>
  )
}

function ValidateIdeaResult({ data, onGrantClick, onFailureClick }) {
  if (!data) return null
  const confColor = {
    HIGH:   'text-navy-800 bg-navy-50 border-navy-200',
    MEDIUM: 'text-amber-700 bg-amber-50 border-amber-200',
    LOW:    'text-red-700 bg-red-50 border-red-200',
  }[data.confidence] || 'text-grey-600 bg-grey-100 border-grey-200'

  return (
    <div className="p-6 space-y-5">
      <div className="flex items-center gap-3">
        <div className={`shrink-0 px-3 py-1.5 rounded-full text-sm font-bold border ${confColor}`}>
          {data.confidence}
        </div>
        <div className="flex items-center gap-2">
          <div className={`w-3 h-3 rounded-full shrink-0 ${
            data.market_exists === true ? 'bg-navy-700' : data.market_exists === false ? 'bg-red-400' : 'bg-grey-300'
          }`} />
          <span className="text-base font-semibold text-grey-700">
            {data.market_exists === true ? 'Market exists' : data.market_exists === false ? 'Market unclear' : 'Inconclusive'}
          </span>
        </div>
      </div>

      <div>
        <p className="section-label mb-2">Verdict</p>
        <p className="text-base text-grey-700 leading-relaxed">{data.verdict}</p>
      </div>

      {data.biggest_risk && (
        <div className="bg-red-50 border border-red-100 rounded-xl p-4">
          <div className="flex items-center gap-2 mb-2">
            <AlertTriangle size={14} className="text-red-500" />
            <p className="text-xs font-bold text-red-600 uppercase tracking-wider">Biggest risk</p>
          </div>
          <p className="text-base text-red-800">{data.biggest_risk}</p>
        </div>
      )}

      {data.first_grant && data.first_grant !== 'None found' && (
        <div className="bg-navy-50 border border-navy-100 rounded-xl p-4">
          <p className="section-label mb-2 text-navy-700">Apply to first</p>
          <p className="text-base font-bold text-navy-900">{data.first_grant}</p>
        </div>
      )}

      {data.matching_grants?.length > 0 && (
        <div>
          <p className="section-label mb-3">Matching grants ({data.matching_grants.length})</p>
          <div className="space-y-2.5">
            {data.matching_grants.map((g, i) => (
              <button key={g.opp_id || i}
                onClick={() => g.opp_id && onGrantClick && onGrantClick(g.opp_id)}
                className={`w-full text-left flex items-center justify-between bg-grey-50 rounded-xl px-4 py-3 border border-grey-100 transition-all group
                  ${g.opp_id ? 'hover:border-navy-300 hover:bg-navy-50 cursor-pointer' : 'cursor-default'}`}>
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-bold text-grey-800 line-clamp-1 group-hover:text-navy-900">{g.title}</p>
                  <p className="text-xs text-grey-400 mt-0.5 font-medium">{g.agency}</p>
                </div>
                <div className="flex items-center gap-2 shrink-0 ml-3">
                  {fmt(g.funding_max) && (
                    <span className="text-sm font-bold text-navy-800">{fmt(g.funding_max)}</span>
                  )}
                  {g.opp_id && <ExternalLink size={12} className="text-grey-300 group-hover:text-navy-600" />}
                </div>
              </button>
            ))}
          </div>
          {data.matching_grants.some(g => g.opp_id) && (
            <p className="text-xs text-grey-400 mt-2 font-medium">↗ Click a grant to open it in Findout</p>
          )}
        </div>
      )}

      {data.known_failures?.length > 0 && (
        <div>
          <p className="section-label mb-3">Known failures in this space</p>
          <div className="space-y-2">
            {data.known_failures.map((f, i) => (
              <button key={f.failure_id || i}
                onClick={() => f.failure_id && onFailureClick && onFailureClick(f.failure_id)}
                className={`w-full text-left bg-red-50 rounded-xl px-4 py-3 border border-red-100 transition-all group
                  ${f.failure_id ? 'hover:border-red-300 hover:bg-red-100 cursor-pointer' : 'cursor-default'}`}>
                <div className="flex items-center justify-between mb-1">
                  <p className="text-sm font-bold text-red-800 group-hover:text-red-900">{f.company_name}</p>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-red-400 font-medium">{f.year_failed}</span>
                    {f.failure_id && <ExternalLink size={11} className="text-red-300 group-hover:text-red-600" />}
                  </div>
                </div>
                {f.key_lesson && (
                  <p className="text-xs text-red-600 italic line-clamp-1">{f.key_lesson}</p>
                )}
              </button>
            ))}
          </div>
          {data.known_failures.some(f => f.failure_id) && (
            <p className="text-xs text-grey-400 mt-2 font-medium">↗ Click a failure to read the full post-mortem</p>
          )}
        </div>
      )}
    </div>
  )
}

function GapFinderResult({ data }) {
  if (!data?.analysis) return null
  const { analysis, signals } = data
  const cells = [
    { key: 'tried_and_failed',        label: 'Tried & failed',         color: 'red'   },
    { key: 'researched_not_funded',   label: 'Researched, not funded', color: 'amber' },
    { key: 'open_demand',             label: 'Open demand',            color: 'blue'  },
    { key: 'non_obvious_opportunity', label: 'Non-obvious play',       color: 'navy'  },
  ]
  const cellStyles = {
    red:   { wrap: 'bg-red-50 border-red-100',       label: 'text-red-600',   body: 'text-red-800'    },
    amber: { wrap: 'bg-amber-50 border-amber-100',   label: 'text-amber-600', body: 'text-amber-800'  },
    blue:  { wrap: 'bg-blue-50 border-blue-100',     label: 'text-blue-600',  body: 'text-blue-800'   },
    navy:  { wrap: 'bg-navy-50 border-navy-200',     label: 'text-navy-700',  body: 'text-navy-900'   },
  }
  return (
    <div className="p-6 space-y-5">
      {signals && (
        <div className="flex items-center gap-5 text-sm text-grey-600 bg-grey-50 rounded-xl px-5 py-3 border border-grey-100 flex-wrap">
          <span>Innovation: <b className="text-purple-700 font-bold">{signals.innovation}</b></span>
          <span className="text-grey-300">|</span>
          <span>Market: <b className="text-blue-700 font-bold">{signals.market}</b></span>
          <span className="text-grey-300">|</span>
          <span>Gap score: <b className={`font-bold ${signals.gap_score > 20 ? 'text-navy-800' : 'text-grey-700'}`}>{signals.gap_score}</b></span>
        </div>
      )}
      <div className="grid grid-cols-2 gap-3">
        {cells.map(({ key, label, color }) => {
          const s = cellStyles[color]
          return (
            <div key={key} className={`rounded-xl border p-4 ${s.wrap} ${color === 'navy' ? 'ring-1 ring-navy-200' : ''}`}>
              <p className={`text-xs font-bold uppercase tracking-wider mb-2.5 flex items-center gap-1.5 ${s.label}`}>
                {label}{color === 'navy' && <Zap size={11} />}
              </p>
              <p className={`text-sm leading-relaxed font-medium ${s.body}`}>{analysis[key] || '—'}</p>
            </div>
          )
        })}
      </div>
    </div>
  )
}

function GrantMatchResult({ data, onGrantClick }) {
  if (!data) return null
  return (
    <div className="p-6 space-y-5">
      <div className="flex items-start justify-between gap-4">
        <div className="flex-1">
          <p className="section-label mb-2">Best match</p>
          <p className="text-base font-bold text-grey-900">{data.best_match}</p>
        </div>
        <div className="shrink-0 flex flex-col items-center">
          <div className="w-14 h-14 rounded-full flex items-center justify-center bg-navy-50 border-2 border-navy-200">
            <span className="text-base font-bold text-navy-800">{data.fit_score}/10</span>
          </div>
          <p className="text-xs text-grey-400 mt-1.5 font-medium">Fit score</p>
        </div>
      </div>
      {data.fit_score != null && (
        <div className="h-2.5 bg-grey-100 rounded-full overflow-hidden">
          <div className="h-2.5 rounded-full bg-navy-700 transition-all duration-700"
            style={{ width: `${data.fit_score * 10}%` }} />
        </div>
      )}
      {data.met_criteria?.length > 0 && (
        <div>
          <p className="section-label mb-2.5">You meet</p>
          <div className="flex flex-wrap gap-2">
            {data.met_criteria.map((c, i) => <span key={i} className="chip chip-navy">{c}</span>)}
          </div>
        </div>
      )}
      {data.missing?.length > 0 && (
        <div>
          <p className="section-label mb-2.5">Missing from description</p>
          <div className="flex flex-wrap gap-2">
            {data.missing.map((m, i) => <span key={i} className="chip chip-red">{m}</span>)}
          </div>
        </div>
      )}
      {data.ranked_grants?.length > 0 && (
        <div>
          <p className="section-label mb-2.5">All matches ranked</p>
          <div className="space-y-2.5">
            {data.ranked_grants.map((g, i) => {
              const full = data.grants?.find(gr => gr.title === g.title)
              return (
                <button key={i}
                  onClick={() => full?.opp_id && onGrantClick && onGrantClick(full.opp_id)}
                  className={`w-full text-left rounded-xl px-4 py-3 border transition-all group
                    ${i === 0 ? 'bg-navy-50 border-navy-200' : 'bg-grey-50 border-grey-100'}
                    ${full?.opp_id ? 'hover:border-navy-400 cursor-pointer' : 'cursor-default'}`}>
                  <div className="flex items-start gap-3">
                    <span className={`text-sm font-bold w-6 shrink-0 mt-0.5 ${i === 0 ? 'text-navy-700' : 'text-grey-400'}`}>#{i+1}</span>
                    <div className="flex-1 min-w-0">
                      <p className={`text-sm font-bold ${i === 0 ? 'text-navy-900' : 'text-grey-700'}`}>{g.title}</p>
                      {g.fit_note && <p className={`text-xs mt-0.5 font-medium ${i === 0 ? 'text-navy-700' : 'text-grey-400'}`}>{g.fit_note}</p>}
                    </div>
                    {full?.opp_id && <ExternalLink size={12} className="text-grey-300 group-hover:text-navy-600 shrink-0 mt-0.5" />}
                  </div>
                </button>
              )
            })}
          </div>
          {data.grants?.some(g => g.opp_id) && (
            <p className="text-xs text-grey-400 mt-2 font-medium">↗ Click a grant to open it in Findout</p>
          )}
        </div>
      )}
    </div>
  )
}

export default function Intelligence() {
  const navigate = useNavigate()
  const [query, setQuery]               = useState('')
  const [submitted, setSubmitted]       = useState('')
  const [loading, setLoading]           = useState(false)
  const [error, setError]               = useState(null)
  const [validateData, setValidateData] = useState(null)
  const [gapData, setGapData]           = useState(null)
  const [matchData, setMatchData]       = useState(null)
  const inputRef = useRef(null)

  const handleSearch = async () => {
    const q = query.trim()
    if (!q) return
    setSubmitted(q); setLoading(true); setError(null)
    setValidateData(null); setGapData(null); setMatchData(null)
    try {
      const validateRes = await validateIdea(q)
      setValidateData(validateRes)
      const [gapRes, matchRes] = await Promise.allSettled([
        gapFinder(q.split(' ').slice(0, 3).join(' ')),
        grantMatch(q),
      ])
      if (gapRes.status === 'fulfilled')   setGapData(gapRes.value)
      if (matchRes.status === 'fulfilled') setMatchData(matchRes.value)
    } catch (e) { setError(e.message) }
    finally { setLoading(false) }
  }

  const handleClear = () => {
    setQuery(''); setSubmitted('')
    setValidateData(null); setGapData(null); setMatchData(null); setError(null)
    setTimeout(() => inputRef.current?.focus(), 50)
  }

  const handleGrantClick = (oppId) => {
    navigate('/home', { state: { openOppId: oppId } })
  }

  const handleFailureClick = (failureId) => {
    navigate('/graveyard', { state: { openFailureId: failureId } })
  }

  const hasResults = validateData || gapData || matchData

  return (
    <div className="flex flex-col h-screen overflow-hidden bg-grey-100">
      <div className={`flex flex-col items-center transition-all duration-500
        ${hasResults || loading ? 'pt-10 pb-6' : 'justify-center flex-1'}`}>

        <div className="flex items-center gap-4 mb-8">
          <div className="w-12 h-12 rounded-xl bg-navy-900 text-white flex items-center justify-center font-bold text-lg">
            💬
          </div>
          <div>
            <h1 className="font-display text-3xl font-bold italic text-grey-900 leading-none">Idea?</h1>
            <p className="text-sm text-grey-400 mt-1 font-medium">Let's validate it against real market data</p>
          </div>
        </div>

        <div className="w-full max-w-2xl px-4">
          <div className="relative flex items-center">
            <Search size={17} className="absolute left-5 text-grey-400 pointer-events-none" />
            <input ref={inputRef}
              className="w-full bg-white border border-grey-200 rounded-full
                         pl-12 pr-32 py-4 text-base text-grey-900 font-medium
                         placeholder-grey-400 shadow-sm
                         focus:outline-none focus:border-navy-400 focus:ring-2 focus:ring-navy-100 transition-all"
              value={query} onChange={e => setQuery(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleSearch()}
              placeholder="Describe your startup idea…" />
            {query && (
              <button onClick={handleClear} className="absolute right-28 text-grey-400 hover:text-grey-600 text-xl font-light">×</button>
            )}
            <button onClick={handleSearch} disabled={loading || !query.trim()}
              className="absolute right-2 btn-primary rounded-full px-5 py-2 text-sm
                         disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2">
              {loading ? <RefreshCw size={13} className="animate-spin" /> : <Sparkles size={13} />}
              Analyse
            </button>
          </div>
          {submitted && !loading && (
            <p className="text-sm text-grey-400 mt-3 text-center font-medium">
              Results for <span className="text-grey-700 font-bold">"{submitted}"</span>
              <button onClick={handleClear} className="ml-2 text-navy-700 hover:text-navy-900 underline font-bold">clear</button>
            </p>
          )}
        </div>
      </div>

      {(hasResults || loading || error) && (
        <div className="flex-1 overflow-y-auto px-4 pb-10">
          <div className="max-w-2xl mx-auto space-y-4">
            {error && (
              <div className="bg-red-50 border border-red-200 rounded-xl px-5 py-4 text-base text-red-700 font-medium">{error}</div>
            )}
            {loading && !validateData ? (
              <div className="bg-white border border-grey-200 rounded-2xl p-10 flex items-center justify-center gap-3">
                <Spinner size={20} /><span className="text-base text-grey-500 font-medium">Analysing idea…</span>
              </div>
            ) : validateData && (
              <Section title="Idea validation" icon={TrendingUp} defaultOpen={true}>
                <ValidateIdeaResult
                  data={validateData}
                  onGrantClick={handleGrantClick}
                  onFailureClick={handleFailureClick}
                />
              </Section>
            )}
            {loading && validateData && !gapData ? (
              <div className="bg-white border border-grey-200 rounded-2xl p-6 flex items-center gap-3">
                <Spinner size={16} /><span className="text-sm text-grey-400 font-medium">Finding market gaps…</span>
              </div>
            ) : gapData && (
              <Section title="Market gap analysis" icon={Zap} defaultOpen={false}>
                <GapFinderResult data={gapData} />
              </Section>
            )}
            {loading && validateData && !matchData ? (
              <div className="bg-white border border-grey-200 rounded-2xl p-6 flex items-center gap-3">
                <Spinner size={16} /><span className="text-sm text-grey-400 font-medium">Matching grants…</span>
              </div>
            ) : matchData && (
              <Section title="Grant application fit" icon={Sparkles} defaultOpen={false}>
                <GrantMatchResult data={matchData} onGrantClick={handleGrantClick} />
              </Section>
            )}
          </div>
        </div>
      )}
    </div>
  )
}