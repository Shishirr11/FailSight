import { NavLink } from 'react-router-dom'
import { Home, Bookmark, FlaskConical, ChevronLeft, ChevronRight, Zap } from 'lucide-react'

const LINKS = [
  { to: '/home',         icon: Home,         label: 'Home'             },
  { to: '/watchlist',    icon: Bookmark,     label: 'Watchlist'        },
  { to: '/trial-errors', icon: FlaskConical, label: 'Trial & Errors'   },
]

export default function Sidebar({ open, onToggle }) {
  return (
    <aside
      className={`
        relative flex flex-col h-screen shrink-0
        bg-surface-900 border-r border-white/[0.06]
        transition-all duration-200 ease-in-out
        ${open ? 'w-52' : 'w-14'}
      `}
    >
      {}
      <div className={`flex items-center gap-2.5 px-4 py-5 border-b border-white/[0.06] ${!open && 'justify-center px-0'}`}>
        <div className="shrink-0 w-7 h-7 rounded-lg bg-brand-600 flex items-center justify-center">
          <Zap size={14} className="text-white" />
        </div>
        {open && (
          <div className="min-w-0">
            <p className="text-sm font-bold text-slate-100 tracking-tight leading-none">Findout</p>
            <p className="text-2xs text-slate-500 mt-0.5">Intelligence Platform</p>
          </div>
        )}
      </div>

      {}
      <nav className="flex-1 px-2 py-3 space-y-0.5">
        {LINKS.map(({ to, icon: Icon, label }) => (
          <NavLink
            key={to}
            to={to}
            title={!open ? label : undefined}
            className={({ isActive }) =>
              `flex items-center gap-3 px-2.5 py-2.5 rounded-lg text-sm font-medium
               transition-colors group relative
               ${isActive
                 ? 'bg-brand-600/15 text-brand-400'
                 : 'text-slate-500 hover:bg-white/[0.05] hover:text-slate-200'
               }
               ${!open && 'justify-center px-0'}`
            }
          >
            {({ isActive }) => (
              <>
                <Icon size={16} className={`shrink-0 ${isActive ? 'text-brand-400' : ''}`} />
                {open && <span className="truncate">{label}</span>}

                {}
                {isActive && (
                  <span className="absolute left-0 top-1/2 -translate-y-1/2 w-0.5 h-5
                                   bg-brand-500 rounded-r-full" />
                )}
              </>
            )}
          </NavLink>
        ))}
      </nav>

      {}
      <button
        onClick={onToggle}
        className="flex items-center justify-center mx-auto mb-4
                   w-7 h-7 rounded-lg bg-white/[0.05] hover:bg-white/[0.1]
                   text-slate-400 hover:text-slate-200 transition-colors"
        title={open ? 'Collapse sidebar' : 'Expand sidebar'}
      >
        {open ? <ChevronLeft size={13} /> : <ChevronRight size={13} />}
      </button>
    </aside>
  )
}