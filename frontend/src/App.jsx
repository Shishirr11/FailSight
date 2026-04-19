import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { useState } from 'react'
import Sidebar        from './components/layout/Sidebar'
import Home           from './pages/Home'
import Intelligence   from './pages/Intelligence'
import IdeaGraveyard  from './pages/Ideagraveyard'


export default function App() {
  const [sidebarOpen, setSidebarOpen] = useState(true)

  return (
    <BrowserRouter>
      <div className="flex h-screen overflow-hidden bg-grey-50">
        <Sidebar open={sidebarOpen} onToggle={() => setSidebarOpen(o => !o)} />
        <main className="flex-1 overflow-hidden min-w-0">
          <Routes>
            <Route path="/"               element={<Navigate to="/home" replace />} />
            <Route path="/home"           element={<Home />} />
            <Route path="/intelligence"   element={<Intelligence />} />
            <Route path="/graveyard"      element={<IdeaGraveyard />} />
            <Route path="*"               element={<Navigate to="/home" replace />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  )
}