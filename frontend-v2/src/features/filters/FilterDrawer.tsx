import { useState, useEffect, useMemo } from 'react';
import Select from 'react-select';
import type { StylesConfig } from 'react-select';
import { useFilters, DEFAULT_FILTERS, needsFilteredPath } from './store';
import type { Team } from '../../types';

interface TeamOption {
  value: number;
  label: string;
}

const GAME_TYPE_OPTIONS: TeamOption[] = [
  { value: 5, label: 'Regular Season' },
  { value: 16, label: 'Playoffs QF' },
  { value: 26, label: 'Playoffs SF' },
  { value: 17, label: 'Playoffs Finals' },
  { value: 33, label: 'Play-in' },
  { value: 34, label: 'Winner Cup' },
];

const selectStyles: StylesConfig<TeamOption, true> = {
  control: (base, state) => ({
    ...base,
    background: 'var(--bg-elevated)',
    borderColor: state.isFocused ? 'var(--accent)' : 'var(--bg-hover)',
    boxShadow: state.isFocused ? '0 0 0 1px var(--accent)' : 'none',
    minHeight: 32,
    fontSize: 13,
    '&:hover': { borderColor: 'var(--accent)' },
  }),
  menu: (base) => ({
    ...base,
    background: 'var(--bg-elevated)',
    border: '1px solid var(--bg-hover)',
    zIndex: 20,
  }),
  option: (base, state) => ({
    ...base,
    background: state.isFocused ? 'var(--bg-hover)' : 'transparent',
    color: 'var(--text-primary)',
    fontSize: 13,
    cursor: 'pointer',
    '&:active': { background: 'var(--bg-active)' },
  }),
  multiValue: (base) => ({
    ...base,
    background: 'var(--bg-active)',
    borderRadius: 4,
  }),
  multiValueLabel: (base) => ({
    ...base,
    color: 'var(--text-primary)',
    fontSize: 12,
  }),
  multiValueRemove: (base) => ({
    ...base,
    color: 'var(--text-muted)',
    '&:hover': { background: 'var(--negative)', color: '#fff' },
  }),
  input: (base) => ({
    ...base,
    color: 'var(--text-primary)',
  }),
  placeholder: (base) => ({
    ...base,
    color: 'var(--text-muted)',
    fontSize: 13,
  }),
  noOptionsMessage: (base) => ({
    ...base,
    color: 'var(--text-muted)',
  }),
};

export default function FilterDrawer() {
  const { state, dispatch, drawerOpen, setDrawerOpen } = useFilters();
  const [teams, setTeams] = useState<Team[]>([]);
  const [gnValues, setGnValues] = useState<number[]>([]);

  // Fetch teams for dropdown
  useEffect(() => {
    fetch(`/api/meta/teams?game_year=${state.gameYear}`)
      .then(r => r.json())
      .then(setTeams)
      .catch(() => {});
  }, [state.gameYear]);

  // Fetch game numbers for selectize
  useEffect(() => {
    fetch(`/api/meta/game-numbers?game_year=${state.gameYear}`)
      .then(r => r.json())
      .then((vals: number[]) => setGnValues(vals))
      .catch(() => {});
  }, [state.gameYear]);

  // Escape key closes drawer
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && drawerOpen) setDrawerOpen(false);
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [drawerOpen, setDrawerOpen]);

  const hasActiveFilters = needsFilteredPath(state) ||
    state.minOnPoss !== DEFAULT_FILTERS.minOnPoss ||
    state.minAllPoss !== DEFAULT_FILTERS.minAllPoss ||
    state.lineupPlayersActive;

  // Team options for react-select
  const teamOptions: TeamOption[] = useMemo(
    () => teams.map(t => ({ value: t.teamId, label: t.teamName })),
    [teams],
  );
  const selectedOpponents = useMemo(
    () => teamOptions.filter(o => state.opponents.includes(o.value)),
    [teamOptions, state.opponents],
  );
  const selectedGameTypes = useMemo(
    () => GAME_TYPE_OPTIONS.filter(o => state.gameType.includes(o.value)),
    [state.gameType],
  );

  // Last N choices: 1..max(gn)
  const lastNChoices = useMemo(() => {
    if (!gnValues.length) return [];
    const max = Math.max(...gnValues);
    return Array.from({ length: max }, (_, i) => i + 1);
  }, [gnValues]);

  return (
    <aside className={`filter-drawer ${drawerOpen ? 'open' : ''}`}>
      <div className="drawer-header">
        <span className="drawer-title">Filters</span>
        <button className="drawer-close" onClick={() => setDrawerOpen(false)}>
          &times;
        </button>
      </div>

      {hasActiveFilters && (
        <button
          className="clear-all-btn prominent"
          onClick={() => dispatch({ type: 'RESET' })}
        >
          Clear all filters
        </button>
      )}

      {/* Time Filters */}
      <FilterSection title="Time Filters" defaultOpen>
        <div className="filter-group">
          <label className="filter-label">Season</label>
          <select
            className="filter-select"
            value={state.gameYear}
            onChange={e =>
              dispatch({ type: 'SET_FIELD', field: 'gameYear', value: parseInt(e.target.value) })
            }
          >
            <option value={2026}>2025-26</option>
            <option value={2025}>2024-25</option>
          </select>
        </div>
        <div className="filter-group">
          <label className="filter-label">Date Range</label>
          <div className="filter-row">
            <input
              type="date"
              className="filter-input"
              value={state.startDate}
              onChange={e =>
                dispatch({ type: 'SET_FIELD', field: 'startDate', value: e.target.value })
              }
            />
            <input
              type="date"
              className="filter-input"
              value={state.endDate}
              onChange={e =>
                dispatch({ type: 'SET_FIELD', field: 'endDate', value: e.target.value })
              }
            />
          </div>
        </div>
      </FilterSection>

      {/* Game Filters */}
      <FilterSection title="Game Filters">
        <div className="filter-group">
          <label className="filter-label">Teams</label>
          <Select<TeamOption, true>
            isMulti
            closeMenuOnSelect={false}
            options={teamOptions}
            value={teamOptions.filter(o => state.teamIds.includes(o.value))}
            onChange={(sel) =>
              dispatch({
                type: 'SET_FIELD',
                field: 'teamIds',
                value: sel ? sel.map(s => s.value) : [],
              })
            }
            placeholder="All teams"
            styles={selectStyles}
            classNamePrefix="rs"
          />
        </div>
        <div className="filter-group">
          <label className="filter-label">Game Type</label>
          <Select<TeamOption, true>
            isMulti
            closeMenuOnSelect={false}
            options={GAME_TYPE_OPTIONS}
            value={selectedGameTypes}
            onChange={(sel) =>
              dispatch({
                type: 'SET_FIELD',
                field: 'gameType',
                value: sel ? sel.map(s => s.value) : [],
              })
            }
            placeholder="All game types"
            styles={selectStyles}
            classNamePrefix="rs"
          />
        </div>
        <div className="filter-group">
          <label className="filter-label">Opponents</label>
          <Select<TeamOption, true>
            isMulti
            closeMenuOnSelect={false}
            options={teamOptions}
            value={selectedOpponents}
            onChange={(sel) =>
              dispatch({
                type: 'SET_FIELD',
                field: 'opponents',
                value: sel ? sel.map(s => s.value) : [],
              })
            }
            placeholder="All opponents"
            styles={selectStyles}
            classNamePrefix="rs"
          />
        </div>
        <div className="filter-row">
          <div className="filter-group">
            <label className="filter-label">Home/Away</label>
            <select
              className="filter-select"
              value={state.homeAway}
              onChange={e =>
                dispatch({ type: 'SET_FIELD', field: 'homeAway', value: e.target.value })
              }
            >
              <option value="">All</option>
              <option value="home">Home</option>
              <option value="away">Away</option>
            </select>
          </div>
          <div className="filter-group">
            <label className="filter-label">Outcome</label>
            <select
              className="filter-select"
              value={state.outcome}
              onChange={e =>
                dispatch({ type: 'SET_FIELD', field: 'outcome', value: e.target.value })
              }
            >
              <option value="">All</option>
              <option value="win">Win</option>
              <option value="loss">Loss</option>
            </select>
          </div>
        </div>
        <div className="filter-group">
          <label className="filter-label">Game Number (GN)</label>
          <div className="filter-row">
            <select
              className="filter-select"
              value={state.gnMin ?? ''}
              onChange={e =>
                dispatch({
                  type: 'SET_FIELD',
                  field: 'gnMin',
                  value: e.target.value ? parseInt(e.target.value) : null,
                })
              }
            >
              <option value="">From</option>
              {gnValues.map(gn => (
                <option key={gn} value={gn}>GN {gn}</option>
              ))}
            </select>
            <select
              className="filter-select"
              value={state.gnMax ?? ''}
              onChange={e =>
                dispatch({
                  type: 'SET_FIELD',
                  field: 'gnMax',
                  value: e.target.value ? parseInt(e.target.value) : null,
                })
              }
            >
              <option value="">To</option>
              {gnValues.map(gn => (
                <option key={gn} value={gn}>GN {gn}</option>
              ))}
            </select>
          </div>
        </div>
        <div className="filter-group">
          <label className="filter-label">Last N Games</label>
          <select
            className="filter-select"
            value={state.lastN ?? ''}
            onChange={e =>
              dispatch({
                type: 'SET_FIELD',
                field: 'lastN',
                value: e.target.value ? parseInt(e.target.value) : null,
              })
            }
          >
            <option value="">Any</option>
            {lastNChoices.map(n => (
              <option key={n} value={n}>{n}</option>
            ))}
          </select>
        </div>
      </FilterSection>

      {/* Opponent Strength */}
      <FilterSection title="Opponent Strength">
        <div className="filter-group">
          <label className="filter-label">Top / Bottom</label>
          <select
            className="filter-select"
            value={state.oppRankSide}
            onChange={e =>
              dispatch({ type: 'SET_FIELD', field: 'oppRankSide', value: e.target.value })
            }
          >
            <option value="">Off</option>
            <option value="top">Top</option>
            <option value="bottom">Bottom</option>
          </select>
        </div>
        <div className="filter-row">
          <div className="filter-group">
            <label className="filter-label">Rank N</label>
            <select
              className="filter-select"
              value={state.oppRankN ?? ''}
              onChange={e =>
                dispatch({
                  type: 'SET_FIELD',
                  field: 'oppRankN',
                  value: e.target.value ? parseInt(e.target.value) : null,
                })
              }
            >
              <option value="">—</option>
              {Array.from({ length: 12 }, (_, i) => i + 1).map(n => (
                <option key={n} value={n}>{n}</option>
              ))}
            </select>
          </div>
          <div className="filter-group">
            <label className="filter-label">Metric</label>
            <select
              className="filter-select"
              value={state.oppRankMetric}
              onChange={e =>
                dispatch({ type: 'SET_FIELD', field: 'oppRankMetric', value: e.target.value })
              }
            >
              <option value="">—</option>
              <option value="off">Offense</option>
              <option value="def">Defense</option>
              <option value="net">Net rating</option>
            </select>
          </div>
        </div>
      </FilterSection>

    </aside>
  );
}

/** Collapsible filter section */
function FilterSection({
  title,
  defaultOpen = false,
  children,
}: {
  title: string;
  defaultOpen?: boolean;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className={`filter-section ${open ? 'open' : ''}`}>
      <div className="filter-section-header" onClick={() => setOpen(!open)}>
        <span className="filter-section-title">{title}</span>
        <span className="filter-section-chevron">&#9660;</span>
      </div>
      <div className="filter-section-body">{children}</div>
    </div>
  );
}
