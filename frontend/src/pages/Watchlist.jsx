import { useState, useEffect } from 'react'
import { Bookmark, Plus, X, Search, Bell, Trash2 } from 'lucide-react'

const BASE = '/api/watchlist'

async function fetchWatchlist() {
  const res = await fetch(BASE)
  if (!res.ok) throw new Error('Failed to load watchlist')
  return res.json()
}

async function createWatchlistItem(item) {
  const res = await fetch(BASE, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(item),
  })
  if (!res.ok) throw new Error('Failed to create watchlist item')
  return res.json()
}

async function deleteWatchlistItem(id) {
  const res = await fetch(`${BASE}/${id}`, { method: 'DELETE' })
  if (!res.ok) throw new Error('Failed to delete watchlist item')
}

const SECTORS = [
  'AI & Machine Learning', 'Cybersecurity', 'Clean Energy', 'Climate Technology',
  'Biotechnology', 'Health Technology', 'Quantum Computing', 'Advanced Manufacturing',
  'Advanced Computing', 'Aerospace & Defense', 'Agriculture Technology', 'Fintech',
]

const SOURCES = ['grants', 'sam', 'patents', 'research']

export default function Watchlist() {
  const [items,   setItems]   = useState([])
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState(null)
  const [showNew, setShowNew] = useState(false)

  const [label,      setLabel]      = useState('')
  const [keyword,    setKeyword]    = useState('')
  const [sectors,    setSectors]    = useState([])
  const [minFunding, setMinFunding] = useState('')
  const [sources,    setSources]    = useState([])
  const [saving,     setSaving]     = useState(false)

  useEffect(() => {
    fetchWatchlist()
      .then(data => setItems(data.items || data))
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  const toggleSector = s =>
    setSectors(prev => prev.includes(s) ? prev.filter(x => x !== s) : [...prev, s])

  const toggleSource = s =>
    setSources(prev => prev.includes(s) ? prev.filter(x => x !== s) : [...prev, s])

  const handleSave = async () => {
    if (!label.trim()) return
    setSaving(true)
    try {
      const created = await createWatchlistItem({
        user_label:  label,
        keyword:     keyword || null,
        sectors,
        min_funding: minFunding ? Number(minFunding) : 0,
        sources,
      })
      setItems(prev => [...prev, created])
      setShowNew(false)
      setLabel(''); setKeyword(''); setSectors([]); setMinFunding(''); setSources([])
    } catch (e) {
      setError(e.message)
    } finally {
      setSaving(false)
    }
  }

  const handleDelete = async id => {
    try {
      await deleteWatchlistItem(id)
      setItems(prev => prev.filter(i => i.id !== id))
    } catch (e) {
      setError(e.message)
    }
  }

  return (
    <div className="p-5 max-w-3xl mx-auto space-y-4">

      {}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-xl bg-brand-500/10 border border-brand-500/20
                          flex items-center justify-center">
            <Bookmark size={16} className="text-brand-400" />
          </div>
          <div>
            <h1 className="text-lg font-bold text-slate-100">Watchlist</h1>
            <p className="text-xs text-slate-500">Save searches to monitor for new opportunities</p>
          </div>
        </div>
        <button onClick={() => setShowNew(s => !s)} className="btn-primary flex items-center gap-1.5">
          <Plus size={14} /> New Watch
        </button>
      </div>

      {}
      {showNew && (
        <div className="card p-4 space-y-4 animate-slide-up">
          <p className="text-sm font-semibold text-slate-200">New Watchlist Item</p>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <p className="section-label mb-1.5">Label *</p>
              <input className="input" value={label} onChange={e => setLabel(e.target.value)}
                placeholder="e.g. AI Grants > $100K" />
            </div>
            <div>
              <p className="section-label mb-1.5">Keyword</p>
              <input className="input" value={keyword} onChange={e => setKeyword(e.target.value)}
                placeholder="e.g. machine learning" />
            </div>
          </div>

          <div>
            <p className="section-label mb-2">Sectors</p>
            <div className="flex flex-wrap gap-1.5">
              {SECTORS.map(s => (
                <button key={s} onClick={() => toggleSector(s)}
                  className={`text-xs px-3 py-1.5 rounded-full border transition-colors ${
                    sectors.includes(s)
                      ? 'bg-brand-600/15 text-brand-400 border-brand-500/30'
                      : 'text-slate-400 border-white/[0.08] hover:border-white/[0.15]'
                  }`}>{s}</button>
              ))}
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <p className="section-label mb-1.5">Min Funding ($)</p>
              <input className="input" type="number" value={minFunding}
                onChange={e => setMinFunding(e.target.value)} placeholder="e.g. 50000" />
            </div>
            <div>
              <p className="section-label mb-2">Sources</p>
              <div className="flex gap-2 flex-wrap">
                {SOURCES.map(s => (
                  <button key={s} onClick={() => toggleSource(s)}
                    className={`text-xs px-3 py-1.5 rounded-full border transition-colors ${
                      sources.includes(s)
                        ? 'bg-brand-600/15 text-brand-400 border-brand-500/30'
                        : 'text-slate-400 border-white/[0.08] hover:border-white/[0.15]'
                    }`}>{s}</button>
                ))}
              </div>
            </div>
          </div>

          <div className="flex gap-2 justify-end pt-1">
            <button onClick={() => setShowNew(false)} className="btn-ghost">Cancel</button>
            <button onClick={handleSave} disabled={!label.trim() || saving}
              className="btn-primary disabled:opacity-40">
              {saving ? 'Saving…' : 'Save Watch'}
            </button>
          </div>
        </div>
      )}

      {}
      {error && (
        <div className="rounded-xl bg-danger-500/10 border border-danger-500/20
                        text-danger-400 px-4 py-3 text-sm">
          {error}
        </div>
      )}

      {}
      {loading ? (
        <div className="text-center py-16 text-slate-500 text-sm">Loading…</div>
      ) : items.length === 0 ? (
        <div className="card flex flex-col items-center justify-center py-16 text-center px-6">
          <Bookmark size={28} className="text-slate-700 mb-3" />
          <p className="text-sm text-slate-400 font-medium mb-1">No watches yet</p>
          <p className="text-xs text-slate-600">
            Create a watch to get notified when new matching opportunities appear.
          </p>
        </div>
      ) : (
        <div className="space-y-2">
          {items.map(item => (
            <div key={item.id} className="card p-4 flex items-start justify-between gap-4">
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-2">
                  <Bell size={13} className="text-brand-400 shrink-0" />
                  <p className="text-sm font-semibold text-slate-100">{item.user_label}</p>
                </div>
                <div className="flex flex-wrap gap-2 text-xs text-slate-500">
                  {item.keyword && (
                    <span className="flex items-center gap-1">
                      <Search size={10} /> {item.keyword}
                    </span>
                  )}
                  {item.min_funding > 0 && (
                    <span className="text-emerald-400">
                      Min ${Number(item.min_funding).toLocaleString()}
                    </span>
                  )}
                  {item.sectors?.length > 0 && (
                    <span className="text-brand-400">{item.sectors.join(', ')}</span>
                  )}
                  {item.sources?.length > 0 && (
                    <span>{item.sources.join(', ')}</span>
                  )}
                </div>
                {item.created_at && (
                  <p className="text-2xs text-slate-600 mt-1.5">
                    Added {new Date(item.created_at).toLocaleDateString()}
                  </p>
                )}
              </div>
              <button onClick={() => handleDelete(item.id)}
                className="btn-ghost p-2 text-slate-600 hover:text-danger-400 shrink-0">
                <Trash2 size={14} />
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}