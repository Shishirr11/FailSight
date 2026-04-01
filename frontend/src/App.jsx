import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { useState } from 'react'
import Sidebar        from './components/layout/Sidebar'
import Home           from './pages/Home'
import Watchlist      from './pages/Watchlist'
import TrialAndErrors from './pages/TrialAndErrors'

export default function App() {
  const [sidebarOpen, setSidebarOpen] = useState(true)

  return (
    <BrowserRouter>
      <div className="flex h-screen overflow-hidden bg-surface-950">

        <Sidebar open={sidebarOpen} onToggle={() => setSidebarOpen(o => !o)} />

        <main className="flex-1 overflow-y-auto min-w-0">
          <Routes>
            <Route path="/"               element={<Navigate to="/home" replace />} />
            <Route path="/home"           element={<Home />} />
            <Route path="/watchlist"      element={<Watchlist />} />
            <Route path="/trial-errors"   element={<TrialAndErrors />} />
            <Route path="*"               element={<Navigate to="/home" replace />} />
          </Routes>
        </main>

      </div>
    </BrowserRouter>
  )
}