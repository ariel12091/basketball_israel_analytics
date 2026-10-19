import { ReactNode, useMemo, useState } from 'react';
import { MainTabs } from '../../features/navigation/MainTabs';
import { FilterPanel } from '../../features/filters/FilterPanel';
import { FilterContext, buildInitialFilterState } from '../../features/filters/store';

interface Props {
  children: ReactNode;
}

export function AppShell({ children }: Props) {
  const [showFilters, setShowFilters] = useState(false);
  const [state, setState] = useState(buildInitialFilterState());

  const ctxValue = useMemo(() => ({ state, setState }), [state]);

  return (
    <FilterContext.Provider value={ctxValue}>
      <div className="app-shell">
        <header className="topbar">
          <div>
            <p className="kicker">IBL ANALYTICS DESK</p>
            <h1 className="title">Court Impact Atlas</h1>
          </div>
          <div className="topbar-right">
            <button
              className={`filter-toggle-btn ${showFilters ? 'active' : ''}`}
              onClick={() => setShowFilters((v) => !v)}
              aria-label={showFilters ? 'Hide filters' : 'Show filters'}
            >
              {showFilters ? 'Hide Filters' : 'Show Filters'}
            </button>
            <div className="updated">Last updated: pending API</div>
          </div>
        </header>
        <MainTabs />
        <div className="content-wrap">
          <main className={`main ${showFilters ? 'with-drawer' : ''}`}>{children}</main>
        </div>
        {showFilters ? <button className="drawer-scrim" onClick={() => setShowFilters(false)} aria-label="Close filters" /> : null}
        <aside className={`filter-drawer ${showFilters ? 'open' : ''}`}>
          <FilterPanel />
        </aside>
      </div>
    </FilterContext.Provider>
  );
}
