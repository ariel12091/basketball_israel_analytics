import { useState, useReducer, useMemo } from 'react';
import MainTabs from '../../features/navigation/MainTabs';
import type { TabId } from '../../features/navigation/MainTabs';
import FilterChips from '../../features/filters/FilterChips';
import FilterDrawer from '../../features/filters/FilterDrawer';
import GlossaryModal from '../../features/navigation/GlossaryModal';
import { FilterContext, filterReducer, DEFAULT_FILTERS } from '../../features/filters/store';
import { useApi } from '../../hooks/useApi';
import OnOffPage from '../../pages/OnOffPage';
import LineupsPage from '../../pages/LineupsPage';
import TeamRatingsPage from '../../pages/TeamRatingsPage';
import GameLogsPage from '../../pages/GameLogsPage';

export default function AppShell() {
  const [activeTab, setActiveTab] = useState<TabId>('onoff');
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [filterState, dispatch] = useReducer(filterReducer, DEFAULT_FILTERS);
  const [glossaryOpen, setGlossaryOpen] = useState(false);
  const lastUpdatedParams = useMemo<Record<string, never>>(() => ({}), []);
  const { data: lastUpdatedMeta } = useApi<{ lastUpdated: string | null }>(
    '/api/meta/last-updated',
    lastUpdatedParams,
  );
  const lastUpdated = lastUpdatedMeta?.lastUpdated ?? null;

  const filterBadge =
    (filterState.teamIds.length > 0 ? 1 : 0) +
    (filterState.opponents.length > 0 ? 1 : 0) +
    (filterState.gameType.length > 0 ? 1 : 0) +
    (filterState.minOnPoss !== DEFAULT_FILTERS.minOnPoss ? 1 : 0) +
    (filterState.homeAway ? 1 : 0) +
    (filterState.outcome ? 1 : 0) +
    (filterState.lastN ? 1 : 0) +
    (filterState.gnMin !== null || filterState.gnMax !== null ? 1 : 0) +
    (filterState.oppRankSide ? 1 : 0) +
    (filterState.startersOffMode && filterState.startersOffValue !== null ? 1 : 0) +
    (filterState.startersDefMode && filterState.startersDefValue !== null ? 1 : 0) +
    (filterState.startDate !== DEFAULT_FILTERS.startDate ||
    filterState.endDate !== DEFAULT_FILTERS.endDate
      ? 1
      : 0);

  return (
    <FilterContext.Provider value={{ state: filterState, dispatch, drawerOpen, setDrawerOpen }}>
      {/* Top Bar */}
      <div className="topbar">
        <div className="topbar-brand">
          <div className="topbar-logo">CI</div>
          <div className="topbar-title">
            Court <span>Impact</span>
          </div>
        </div>
        <div className="topbar-right">
          {lastUpdated && (
            <div className="topbar-meta" style={{ fontSize: 11, color: 'var(--text-muted)' }}>
              Updated {lastUpdated}
            </div>
          )}
          <div className="topbar-meta">
            Season <strong>{filterState.gameYear - 1}-{String(filterState.gameYear).slice(2)}</strong>
          </div>
          <button
            className="glossary-btn"
            onClick={() => setGlossaryOpen(true)}
          >
            Glossary
          </button>
          <button
            className={`filter-toggle-btn ${drawerOpen ? 'active' : ''}`}
            onClick={() => setDrawerOpen(!drawerOpen)}
          >
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M22 3H2l8 9.46V19l4 2v-8.54L22 3z" />
            </svg>
            Filters
            {filterBadge > 0 && <span className="filter-badge">{filterBadge}</span>}
          </button>
        </div>
      </div>

      {/* Tab Navigation */}
      <MainTabs activeTab={activeTab} onTabChange={setActiveTab} />

      {/* Active Filter Chips */}
      <FilterChips />

      {/* Content Area */}
      <div className="content-wrap">
        <div className={`main-content ${drawerOpen ? 'with-drawer' : ''}`}>
          {activeTab === 'onoff' && <OnOffPage />}
          {activeTab === 'lineups' && <LineupsPage />}
          {activeTab === 'teams' && <TeamRatingsPage />}
          {activeTab === 'gamelogs' && <GameLogsPage />}
        </div>

        <FilterDrawer />
      </div>

      {glossaryOpen && <GlossaryModal onClose={() => setGlossaryOpen(false)} />}
    </FilterContext.Provider>
  );
}


