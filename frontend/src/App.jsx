import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { useState } from 'react'
import Sidebar       from './components/layout/Sidebar'
import Landing       from './pages/Landing'
import Home          from './pages/Home'
import Intelligence  from './pages/Intelligence'
import IdeaGraveyard from './pages/Ideagraveyard'

function AppShell() {
  const [sidebarOpen, setSidebarOpen] = useState(true)
  return (
    <div className="flex h-screen overflow-hidden bg-grey-100">
      <Sidebar open={sidebarOpen} onToggle={() => setSidebarOpen(o => !o)} />
      <main className="flex-1 overflow-hidden min-w-0">
        <Routes>
          <Route path="/home"         element={<Home />}         />
          <Route path="/intelligence" element={<Intelligence />} />
          <Route path="/graveyard"    element={<IdeaGraveyard />}/>
          <Route path="*"             element={<Navigate to="/home" replace />} />
        </Routes>
      </main>
    </div>
  )
}

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/"   element={<Landing />} />
        <Route path="/*"  element={<AppShell />} />
      </Routes>
    </BrowserRouter>
  )
}