import { useNavigate } from 'react-router-dom'
import { useState, useEffect } from 'react'
import { ArrowRight, Search, Cpu, Skull, TrendingUp, Shield, Zap, BarChart3 } from 'lucide-react'

const FEATURES = [
  {
    icon: Search,
    label: 'Findout',
    route: '/home',
    color: 'text-blue-400',
    bg: 'bg-blue-500/10',
    headline: 'Browse real funding',
    desc: 'Search grants, SAM contracts, SBIR awards, patents and research papers in plain English. TF-IDF semantic search across 30,000+ live opportunities. Click any card for funding details, agency contacts and AI relevance analysis.',
  },
  {
    icon: Cpu,
    label: 'Intelligence',
    route: '/intelligence',
    color: 'text-purple-400',
    bg: 'bg-purple-500/10',
    headline: 'Validate your idea',
    desc: 'Type your startup idea. Get back: does this market exist, what is the biggest risk, the best grant to apply for first and a 2×2 gap finder showing what has been tried and failed vs what is quietly in demand. All grounded in real data.',
  },
  {
    icon: Skull,
    label: 'Idea Graveyard',
    route: '/graveyard',
    color: 'text-red-400',
    bg: 'bg-red-500/10',
    headline: 'Learn from failure',
    desc: '2,700+ documented startup failures with full post-mortems. Filter by sector, failure reason, year and funding raised. Every entry has a key lesson from someone who already spent the money figuring this out.',
  },
]

const STATS = [
  { value: '30,000+', label: 'Active opportunities' },
  { value: '2,700+',  label: 'Failure post-mortems' },
  { value: '8',       label: 'Live data sources'    },
  { value: 'Free',    label: 'No paywall, no signup' },
]

function Counter({ target, suffix = '' }) {
  const [val, setVal] = useState(0)
  const num = parseInt(target.replace(/\D/g, ''))
  useEffect(() => {
    if (!num) { setVal(target); return }
    let start = 0
    const step = Math.ceil(num / 40)
    const t = setInterval(() => {
      start = Math.min(start + step, num)
      setVal(start)
      if (start >= num) clearInterval(t)
    }, 28)
    return () => clearInterval(t)
  }, [num, target])
  if (!num) return <span>{target}</span>
  return <span>{val.toLocaleString()}{suffix}</span>
}

export default function Landing() {
  const navigate = useNavigate()

  return (
    <div className="min-h-screen bg-grey-100 font-sans">

      <section
        className="relative overflow-hidden"
        style={{ background: 'linear-gradient(135deg, #0f172a 0%, #1e293b 40%, #1e3a8a 100%)' }}
      >
        <div className="absolute inset-0 opacity-[0.04]"
          style={{
            backgroundImage: 'linear-gradient(rgba(255,255,255,0.8) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,0.8) 1px,transparent 1px)',
            backgroundSize: '48px 48px',
          }}
        />
        <div className="absolute -top-32 -right-32 w-[600px] h-[600px] rounded-full opacity-[0.07]"
          style={{ background: 'radial-gradient(circle, #60a5fa, transparent 70%)' }}
        />

        <div className="relative max-w-5xl mx-auto px-6 pt-20 pb-24 text-center">
          <div className="flex items-center justify-center gap-3 mb-10">
            <img src="/brain-logo.png" alt="Failsight" className="w-10 h-10 object-contain" />
            <span className="text-white text-xl font-bold tracking-tight">Failsight</span>
          </div>

          <h1 className="font-display text-5xl md:text-6xl font-bold text-white leading-[1.1] mb-6">
            Know what's been built,<br />
            <span className="italic text-navy-300">what's failed,</span><br />
            and where the real<br />opportunity is.
          </h1>

          <p className="text-grey-300 text-lg md:text-xl max-w-2xl mx-auto mb-10 leading-relaxed font-medium">
            One platform that pulls grants, contracts, patents, research papers and
            2,700+ startup failure post-mortems into a single searchable database
            so you can make faster, better-informed decisions before you commit.
          </p>

          <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
            <button
              onClick={() => navigate('/home')}
              className="flex items-center gap-2.5 bg-white text-navy-900 font-bold text-base
                         px-8 py-4 rounded-xl shadow-lg hover:shadow-xl hover:scale-[1.02]
                         transition-all duration-200"
            >
              Get started <ArrowRight size={18} />
            </button>
            <button
              onClick={() => navigate('/intelligence')}
              className="flex items-center gap-2.5 bg-white/10 hover:bg-white/15 text-white
                         font-semibold text-base px-8 py-4 rounded-xl border border-white/20
                         transition-all duration-200"
            >
              <Zap size={16} /> Validate my idea
            </button>
          </div>

          {/* Stats */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-16 max-w-3xl mx-auto">
            {STATS.map(s => (
              <div key={s.label} className="bg-white/5 border border-white/10 rounded-2xl px-4 py-5">
                <p className="text-white text-2xl font-bold tabular-nums">
                  <Counter target={s.value} />
                </p>
                <p className="text-grey-400 text-sm font-medium mt-1">{s.label}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="max-w-5xl mx-auto px-6 py-20">
        <div className="text-center mb-14">
          <p className="text-navy-700 text-sm font-bold uppercase tracking-widest mb-3">What's inside</p>
          <h2 className="font-display text-3xl md:text-4xl font-bold text-grey-900">
            Three tools, one purpose
          </h2>
          <p className="text-grey-500 text-lg mt-3 max-w-xl mx-auto">
            From funding discovery to failure analysis to market validation everything a founder needs before writing a single line of code.
          </p>
        </div>

        <div className="grid md:grid-cols-3 gap-6">
          {FEATURES.map(f => (
            <div
              key={f.label}
              onClick={() => navigate(f.route)}
              className="group bg-white border border-grey-200 rounded-2xl p-7 cursor-pointer
                         hover:border-navy-300 hover:shadow-md transition-all duration-200
                         flex flex-col"
            >
              <div className={`w-12 h-12 rounded-xl ${f.bg} flex items-center justify-center mb-5`}>
                <f.icon size={22} className={f.color} />
              </div>
              <div className="flex items-center justify-between mb-2">
                <span className="text-xs font-bold uppercase tracking-widest text-grey-400">{f.label}</span>
                <ArrowRight size={14} className="text-grey-300 group-hover:text-navy-600 group-hover:translate-x-1 transition-all" />
              </div>
              <h3 className="text-xl font-bold text-grey-900 mb-3">{f.headline}</h3>
              <p className="text-base text-grey-500 leading-relaxed flex-1">{f.desc}</p>
              <button
                className="mt-6 w-full btn-primary py-3 text-sm flex items-center justify-center gap-2"
              >
                Open {f.label} <ArrowRight size={14} />
              </button>
            </div>
          ))}
        </div>
      </section>

      <section className="bg-white border-y border-grey-200">
        <div className="max-w-5xl mx-auto px-6 py-16">
          <p className="text-center text-sm font-bold uppercase tracking-widest text-grey-400 mb-8">
            Data pulled from public sources
          </p>
          <div className="flex flex-wrap justify-center gap-3">
            {['Grants.gov', 'SAM.gov', 'SBIR.gov', 'NSF Awards', 'OpenAlex',
              'PubMed', 'PatentsView', 'CB Insights', 'Failory', 'LootDrop'].map(src => (
              <span key={src}
                className="bg-grey-100 text-grey-600 border border-grey-200 rounded-full
                           px-4 py-1.5 text-sm font-semibold">
                {src}
              </span>
            ))}
          </div>
        </div>
      </section>
    </div>
  )
}