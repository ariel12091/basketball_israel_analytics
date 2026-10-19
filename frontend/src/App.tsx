import { Navigate, Route, Routes } from 'react-router-dom';
import { AppShell } from './app/layout/AppShell';
import { OnOffPage } from './pages/OnOffPage';
import { LineupsPage } from './pages/LineupsPage';
import { TeamsPage } from './pages/TeamsPage';
import { GameLogsPage } from './pages/GameLogsPage';

export function App() {
  return (
    <AppShell>
      <Routes>
        <Route path="/" element={<Navigate to="/onoff" replace />} />
        <Route path="/onoff" element={<OnOffPage />} />
        <Route path="/lineups" element={<LineupsPage />} />
        <Route path="/teams" element={<TeamsPage />} />
        <Route path="/gamelogs" element={<GameLogsPage />} />
      </Routes>
    </AppShell>
  );
}
