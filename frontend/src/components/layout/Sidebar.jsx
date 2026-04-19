import { NavLink } from 'react-router-dom'
import { Home, Cpu, Skull, ChevronLeft, ChevronRight } from 'lucide-react'

const LINKS = [
  { to: '/home',         icon: Home,  label: 'Findout'        },
  { to: '/intelligence', icon: Cpu,   label: 'Intelligence'   },
  { to: '/graveyard',    icon: Skull, label: 'Death of Ideas' },
]

export default function Sidebar({ open, onToggle }) {
  return (
    <aside className={`
      relative flex flex-col h-screen shrink-0 bg-white
      border-r border-grey-200 transition-all duration-200 ease-in-out
      ${open ? 'w-56' : 'w-16'}
    `}>

      <div className={`
        flex items-center gap-3 px-4 py-5 border-b border-grey-100
        ${!open && 'justify-center px-0'}
      `}>
     <img src="/brain-logo.png" alt="Failsight" className="shrink-0 w-8 h-8 object-contain" />        
     {open && (
          <div className="min-w-0">
            <p className="text-base text-3xl font-bold text-grey-900 leading-none tracking-tight font-display">
              Failsight
            </p>
            <p className="text-xs text-grey-400 mt-0.5 font-medium">Founders Intelligence</p>
          </div>
        )}
      </div>

      <nav className="flex-1 px-2 py-3 space-y-1">
        {LINKS.map(({ to, icon: Icon, label }) => (
          <NavLink
            key={to}
            to={to}
            title={!open ? label : undefined}
            className={({ isActive }) => `
              flex items-center gap-3 px-3 py-3 rounded-xl text-sm font-semibold
              transition-colors relative
              ${isActive
                ? 'bg-green-50 text-green-700'
                : 'text-grey-500 hover:bg-grey-50 hover:text-grey-800'
              }
              ${!open && 'justify-center px-0'}
            `}
          >
            {({ isActive }) => (
              <>
                <Icon size={18} className={`shrink-0 ${isActive ? 'text-green-600' : ''}`} />
                {open && <span className="truncate">{label}</span>}
                {isActive && (
                  <span className="absolute left-0 top-1/2 -translate-y-1/2 w-0.5 h-6
                                   bg-green-500 rounded-r-full" />
                )}
              </>
            )}
          </NavLink>
        ))}
      </nav>

      <button
        onClick={onToggle}
        className="flex items-center justify-center mx-auto mb-5 w-8 h-8 rounded-lg
                   bg-grey-100 hover:bg-grey-200 text-grey-500 hover:text-grey-700
                   transition-colors"
        title={open ? 'Collapse' : 'Expand'}
      >
        {open ? <ChevronLeft size={15} /> : <ChevronRight size={15} />}
      </button>
    </aside>
  )
}