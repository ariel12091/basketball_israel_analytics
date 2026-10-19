import { useContext } from 'react';
import { useLocation } from 'react-router-dom';
import Select from 'react-select';
import { FilterContext } from './store';
import { buildDefaultTabFilters } from './defaults';
import { applyGnLastNMutualExclusion } from './rules';
import { AppRouteTab, BaseFilters } from './types';

function routeToTab(pathname: string): AppRouteTab {
  if (pathname.startsWith('/lineups')) return 'lineups';
  if (pathname.startsWith('/teams')) return 'teams';
  if (pathname.startsWith('/gamelogs')) return 'gamelogs';
  return 'onoff';
}

interface FilterChip {
  key: string;
  label: string;
  onRemove: () => void;
}

const GAME_TYPES = [
  { value: '5', label: 'Regular' },
  { value: '16', label: 'QF' },
  { value: '26', label: 'SF' },
  { value: '17', label: 'Final' },
  { value: '33', label: 'Play-In' },
  { value: '34', label: 'Winner Cup' },
  { value: '35', label: 'State Cup' },
];

const TEAM_OPTIONS = [
  'Hapoel Tel Aviv',
  'Maccabi Tel Aviv',
  'Hapoel Holon',
  'Maccabi Ramat Gan',
  'Kiryat Ata',
  'Ness Ziona',
  'Galil Elyon',
  'Bnei Herzliya',
  'Hapoel Jerusalem',
  'Hapoel Haifa',
  'Ironi Kiryat',
  'Elitzur Netanya',
].map((name) => ({ value: name, label: name }));

const PLAYER_OPTIONS = [
  'Ronaldo Sagu',
  'Adam Ariel',
  'Drew Crawford',
  'Joirdon Nichols',
  'Darion Atkins',
  'Tyler Ennis',
  'Jimmy Clark III',
  'Darius Hannah',
  'Tyreek Smith',
  'Guy Palatin',
].map((name) => ({ value: name, label: name }));

const GN_OPTIONS = Array.from({ length: 40 }, (_, i) => {
  const n = String(i + 1);
  return { value: n, label: n };
});

const LAST_N_OPTIONS = ['3', '5', '7', '10', '12', '15', '20'].map((n) => ({
  value: n,
  label: n,
}));

const RANK_N_OPTIONS = Array.from({ length: 12 }, (_, i) => {
  const n = String(i + 1);
  return { value: n, label: n };
});

const MIN_POSS_OPTIONS = [0, 50, 100, 150, 200, 300, 400, 500, 700, 1000, 1500, 2000].map((n) => ({
  value: n,
  label: String(n),
}));

const CLUTCH_MARGIN_OPTIONS = Array.from({ length: 11 }, (_, i) => ({ value: i, label: String(i) }));
const CLUTCH_MINUTES_OPTIONS = [1, 2, 3, 4, 5].map((n) => ({ value: n, label: String(n) }));
const MULTI_SELECT_PROPS = {
  closeMenuOnSelect: false,
  blurInputOnSelect: false,
  hideSelectedOptions: false,
};

export function FilterPanel() {
  const { state, setState } = useContext(FilterContext);
  const { pathname } = useLocation();
  const tab = routeToTab(pathname);
  const filters = state[tab];
  const onoffFilters = state.onoff;
  const lineupFilters = state.lineups;
  const teamFilters = state.teams;
  const gameLogFilters = state.gamelogs;
  const defaults = buildDefaultTabFilters();

  const setBaseFilter = <K extends keyof BaseFilters>(key: K, value: BaseFilters[K]) => {
    setState((prev) => ({
      ...prev,
      [tab]: applyGnLastNMutualExclusion(prev[tab], { [key]: value }),
    }));
  };

  const clearTabFilters = () => {
    setState((prev) => ({ ...prev, [tab]: defaults[tab] }));
  };

  const toggleGameType = (value: string) => {
    const currentValues = filters.gameType.filter((v) => v !== '');
    const exists = currentValues.includes(value);
    const next = exists ? currentValues.filter((v) => v !== value) : [...currentValues, value];
    setBaseFilter('gameType', next.length ? next : ['']);
  };

  const chips: FilterChip[] = [];
  const current = state[tab];
  const defaultCurrent = defaults[tab];

  if (current.season !== defaultCurrent.season) {
    chips.push({
      key: 'season',
      label: `Season: ${current.season}`,
      onRemove: () => setBaseFilter('season', defaultCurrent.season),
    });
  }
  if (current.dateStart !== defaultCurrent.dateStart || current.dateEnd !== defaultCurrent.dateEnd) {
    chips.push({
      key: 'date',
      label: `Dates: ${current.dateStart} to ${current.dateEnd}`,
      onRemove: () => {
        setBaseFilter('dateStart', defaultCurrent.dateStart);
        setBaseFilter('dateEnd', defaultCurrent.dateEnd);
      },
    });
  }
  if (current.homeAway) {
    chips.push({
      key: 'home-away',
      label: `Home/Away: ${current.homeAway}`,
      onRemove: () => setBaseFilter('homeAway', ''),
    });
  }
  if (current.outcome) {
    chips.push({
      key: 'outcome',
      label: `Outcome: ${current.outcome}`,
      onRemove: () => setBaseFilter('outcome', ''),
    });
  }
  if (current.gnMin || current.gnMax || current.lastN) {
    chips.push({
      key: 'gn',
      label: current.lastN
        ? `Last N Games: ${current.lastN}`
        : `Game Number: ${current.gnMin || 'Any'} to ${current.gnMax || 'Any'}`,
      onRemove: () => {
        setBaseFilter('gnMin', '');
        setBaseFilter('gnMax', '');
        setBaseFilter('lastN', '');
      },
    });
  }
  if (tab === 'onoff' && onoffFilters.teams.length) {
    chips.push({
      key: 'teams',
      label: `Teams: ${onoffFilters.teams.length}`,
      onRemove: () => setState((prev) => ({ ...prev, onoff: { ...prev.onoff, teams: [] } })),
    });
  }
  if (tab === 'lineups' && lineupFilters.team) {
    chips.push({
      key: 'lineup-team',
      label: `Team: ${lineupFilters.team}`,
      onRemove: () => setState((prev) => ({ ...prev, lineups: { ...prev.lineups, team: '' } })),
    });
  }
  if (tab === 'lineups' && (lineupFilters.playersOn.length || lineupFilters.playersOff.length)) {
    chips.push({
      key: 'players',
      label: `Players: on ${lineupFilters.playersOn.length}, off ${lineupFilters.playersOff.length}`,
      onRemove: () => setState((prev) => ({ ...prev, lineups: { ...prev.lineups, playersOn: [], playersOff: [] } })),
    });
  }
  if ((tab === 'lineups' && lineupFilters.clutchEnabled) || (tab === 'teams' && teamFilters.clutchEnabled)) {
    chips.push({
      key: 'clutch',
      label: 'Clutch enabled',
      onRemove: () => {
        if (tab === 'lineups') {
          setState((prev) => ({ ...prev, lineups: { ...prev.lineups, clutchEnabled: false } }));
        } else if (tab === 'teams') {
          setState((prev) => ({ ...prev, teams: { ...prev.teams, clutchEnabled: false } }));
        }
      },
    });
  }
  if (tab === 'gamelogs' && gameLogFilters.team) {
    chips.push({
      key: 'gamelog-team',
      label: `Team: ${gameLogFilters.team}`,
      onRemove: () => setState((prev) => ({ ...prev, gamelogs: { ...prev.gamelogs, team: '' } })),
    });
  }

  return (
    <section className="panel">
      <div className="panel-header">
        <h2 className="panel-title">Filters</h2>
        <button className="ghost-btn" onClick={clearTabFilters}>
          Clear filters
        </button>
      </div>

      {chips.length > 0 ? (
        <div className="chips-row" role="toolbar" aria-label="Applied filters">
          {chips.map((chip) => (
            <button key={chip.key} className="chip" onClick={chip.onRemove} aria-label={`Remove ${chip.label}`}>
              {chip.label} x
            </button>
          ))}
        </div>
      ) : (
        <p className="chips-empty">No active filters</p>
      )}

      {tab === 'lineups' ? (
        <details className="filter-section" open>
          <summary>Lineup Specific</summary>
          <div className="field-row">
            <label className="field">
              <span>Minimum Possessions</span>
              <Select
                classNamePrefix="rs"
                options={MIN_POSS_OPTIONS}
                value={MIN_POSS_OPTIONS.find((opt) => opt.value === lineupFilters.minPoss) ?? null}
                onChange={(selected) =>
                  setState((prev) => ({
                    ...prev,
                    lineups: { ...prev.lineups, minPoss: selected?.value ?? 0 },
                  }))
                }
              />
            </label>
          </div>
          <label className="field">
            <span>Team</span>
            <Select
              classNamePrefix="rs"
              isClearable
              options={TEAM_OPTIONS}
              value={TEAM_OPTIONS.find((opt) => opt.value === lineupFilters.team) ?? null}
              onChange={(selected) =>
                setState((prev) => ({
                  ...prev,
                  lineups: { ...prev.lineups, team: selected?.value ?? '' },
                }))
              }
            />
          </label>
          <label className="field">
            <span>Players On</span>
            <Select
              classNamePrefix="rs"
              isMulti
              {...MULTI_SELECT_PROPS}
              options={PLAYER_OPTIONS.filter((opt) => !lineupFilters.playersOff.includes(opt.value))}
              value={PLAYER_OPTIONS.filter((opt) => lineupFilters.playersOn.includes(opt.value))}
              onChange={(selected) => {
                const playersOn = (selected ?? []).map((s) => s.value);
                const playersOff = state.lineups.playersOff.filter((p) => !playersOn.includes(p));
                setState((prev) => ({ ...prev, lineups: { ...prev.lineups, playersOn, playersOff } }));
              }}
            />
          </label>
          <label className="field">
            <span>Players Off</span>
            <Select
              classNamePrefix="rs"
              isMulti
              {...MULTI_SELECT_PROPS}
              options={PLAYER_OPTIONS.filter((opt) => !lineupFilters.playersOn.includes(opt.value))}
              value={PLAYER_OPTIONS.filter((opt) => lineupFilters.playersOff.includes(opt.value))}
              onChange={(selected) => {
                const playersOff = (selected ?? []).map((s) => s.value);
                const playersOn = state.lineups.playersOn.filter((p) => !playersOff.includes(p));
                setState((prev) => ({ ...prev, lineups: { ...prev.lineups, playersOn, playersOff } }));
              }}
            />
          </label>
        </details>
      ) : null}

      <details className="filter-section" open>
        <summary>Time Filters</summary>
        <div className="field">
          <span>Season</span>
          <div className="chip-toggle-row">
            <button
              className={`toggle-chip ${filters.season === '2026' ? 'is-active' : ''}`}
              onClick={() => setState((prev) => ({ ...prev, [tab]: { ...prev[tab], season: '2026' } }))}
            >
              2025-26
            </button>
            <button
              className={`toggle-chip ${filters.season === '2025' ? 'is-active' : ''}`}
              onClick={() => setState((prev) => ({ ...prev, [tab]: { ...prev[tab], season: '2025' } }))}
            >
              2024-25
            </button>
          </div>
        </div>

        <div className="field-row">
          <label className="field">
            <span>Date From</span>
            <input type="date" value={filters.dateStart} onChange={(e) => setBaseFilter('dateStart', e.target.value)} />
          </label>

          <label className="field">
            <span>Date To</span>
            <input type="date" value={filters.dateEnd} onChange={(e) => setBaseFilter('dateEnd', e.target.value)} />
          </label>
        </div>

        <div className="field-row">
          <label className="field">
            <span>From Game Number</span>
            <Select
              classNamePrefix="rs"
              isClearable
              isDisabled={Boolean(filters.lastN)}
              options={GN_OPTIONS}
              value={GN_OPTIONS.find((opt) => opt.value === filters.gnMin) ?? null}
              onChange={(selected) => setBaseFilter('gnMin', selected?.value ?? '')}
            />
          </label>

          <label className="field">
            <span>To Game Number</span>
            <Select
              classNamePrefix="rs"
              isClearable
              isDisabled={Boolean(filters.lastN)}
              options={GN_OPTIONS}
              value={GN_OPTIONS.find((opt) => opt.value === filters.gnMax) ?? null}
              onChange={(selected) => setBaseFilter('gnMax', selected?.value ?? '')}
            />
          </label>
        </div>
        <label className="field">
          <span>Last N Games</span>
          <Select
            classNamePrefix="rs"
            isClearable
            isDisabled={Boolean(filters.gnMin || filters.gnMax)}
            options={LAST_N_OPTIONS}
            value={LAST_N_OPTIONS.find((opt) => opt.value === filters.lastN) ?? null}
            onChange={(selected) => setBaseFilter('lastN', selected?.value ?? '')}
          />
        </label>
      </details>

      <details className="filter-section">
        <summary>Game Filters</summary>
        {tab === 'onoff' ? (
          <label className="field">
            <span>Teams</span>
            <Select
              classNamePrefix="rs"
              isMulti
              {...MULTI_SELECT_PROPS}
              options={TEAM_OPTIONS}
              value={TEAM_OPTIONS.filter((opt) => onoffFilters.teams.includes(opt.value))}
              onChange={(selected) =>
                setState((prev) => ({
                  ...prev,
                  onoff: { ...prev.onoff, teams: (selected ?? []).map((s) => s.value) },
                }))
              }
            />
          </label>
        ) : null}

        <div className="field">
          <span>Game Types</span>
          <div className="chip-toggle-row">
            {GAME_TYPES.map((gt) => (
              <button
                key={gt.value}
                className={`toggle-chip ${filters.gameType.includes(gt.value) ? 'is-active' : ''}`}
                onClick={() => toggleGameType(gt.value)}
              >
                {gt.label}
              </button>
            ))}
          </div>
        </div>

        <label className="field">
          <span>Opponents</span>
          <Select
            classNamePrefix="rs"
            isMulti
            {...MULTI_SELECT_PROPS}
            options={TEAM_OPTIONS}
            value={TEAM_OPTIONS.filter((opt) => filters.opponents.includes(opt.value))}
            onChange={(selected) => setBaseFilter('opponents', (selected ?? []).map((s) => s.value))}
          />
        </label>

        <div className="field">
          <span>Home/Away</span>
          <div className="chip-toggle-row">
            <button className={`toggle-chip ${filters.homeAway === '' ? 'is-active' : ''}`} onClick={() => setBaseFilter('homeAway', '')}>All</button>
            <button className={`toggle-chip ${filters.homeAway === 'home' ? 'is-active' : ''}`} onClick={() => setBaseFilter('homeAway', 'home')}>Home</button>
            <button className={`toggle-chip ${filters.homeAway === 'away' ? 'is-active' : ''}`} onClick={() => setBaseFilter('homeAway', 'away')}>Away</button>
          </div>
        </div>

        <div className="field">
          <span>Outcome</span>
          <div className="chip-toggle-row">
            <button className={`toggle-chip ${filters.outcome === '' ? 'is-active' : ''}`} onClick={() => setBaseFilter('outcome', '')}>All</button>
            <button className={`toggle-chip ${filters.outcome === 'win' ? 'is-active' : ''}`} onClick={() => setBaseFilter('outcome', 'win')}>Win</button>
            <button className={`toggle-chip ${filters.outcome === 'loss' ? 'is-active' : ''}`} onClick={() => setBaseFilter('outcome', 'loss')}>Loss</button>
          </div>
        </div>
      </details>

      <details className="filter-section">
        <summary>Opponent Strength</summary>
        <div className="field-row">
          <label className="field">
            <span>Top/Bottom</span>
            <select value={filters.oppRankSide} onChange={(e) => setBaseFilter('oppRankSide', e.target.value as '' | 'top' | 'bottom')}>
              <option value="">Off</option>
              <option value="top">Top</option>
              <option value="bottom">Bottom</option>
            </select>
          </label>
          <label className="field">
            <span>Rank N</span>
            <Select
              classNamePrefix="rs"
              isClearable
              options={RANK_N_OPTIONS}
              value={RANK_N_OPTIONS.find((opt) => opt.value === filters.oppRankN) ?? null}
              onChange={(selected) => setBaseFilter('oppRankN', selected?.value ?? '')}
            />
          </label>
        </div>
        <label className="field">
          <span>Metric</span>
          <select value={filters.oppRankMetric} onChange={(e) => setBaseFilter('oppRankMetric', e.target.value as '' | 'off' | 'def' | 'net')}>
            <option value="">Off</option>
            <option value="off">Offense</option>
            <option value="def">Defense</option>
            <option value="net">Net rating</option>
          </select>
        </label>
      </details>

      {tab === 'onoff' ? (
        <section className="filter-section filter-section-static">
          <h3 className="filter-static-title">Eligibility</h3>
          <div className="field-row">
            <label className="field slider-field">
              <span className="slider-head">
                <span>Min Possessions (All)</span>
                <strong className="slider-value">{onoffFilters.minAllPoss}</strong>
              </span>
              <input
                className="pretty-slider"
                type="range"
                min={0}
                max={2000}
                step={10}
                value={onoffFilters.minAllPoss}
                onChange={(e) =>
                  setState((prev) => ({
                    ...prev,
                    onoff: { ...prev.onoff, minAllPoss: Number(e.target.value) },
                  }))
                }
              />
              <span className="slider-ends"><span>0</span><span>2000</span></span>
            </label>
            <label className="field slider-field">
              <span className="slider-head">
                <span>Min ON Possessions</span>
                <strong className="slider-value">{onoffFilters.minOnPoss}</strong>
              </span>
              <input
                className="pretty-slider"
                type="range"
                min={0}
                max={3000}
                step={10}
                value={onoffFilters.minOnPoss}
                onChange={(e) =>
                  setState((prev) => ({
                    ...prev,
                    onoff: { ...prev.onoff, minOnPoss: Number(e.target.value) },
                  }))
                }
              />
              <span className="slider-ends"><span>0</span><span>3000</span></span>
            </label>
          </div>
        </section>
      ) : null}

      {tab === 'teams' ? (
        <details className="filter-section">
          <summary>Team Specific</summary>
          <label className="field checkbox-field">
            <input
              type="checkbox"
              checked={teamFilters.clutchEnabled}
              onChange={(e) => setState((prev) => ({ ...prev, teams: { ...prev.teams, clutchEnabled: e.target.checked } }))}
            />
            <span>Enable clutch filter</span>
          </label>
          {teamFilters.clutchEnabled ? (
            <>
              <div className="field-row">
                <label className="field">
                  <span>Max Margin</span>
                  <Select
                    classNamePrefix="rs"
                    options={CLUTCH_MARGIN_OPTIONS}
                    value={CLUTCH_MARGIN_OPTIONS.find((opt) => opt.value === teamFilters.clutchMargin) ?? null}
                    onChange={(selected) =>
                      setState((prev) => ({ ...prev, teams: { ...prev.teams, clutchMargin: selected?.value ?? 0 } }))
                    }
                  />
                </label>
                <label className="field">
                  <span>Max Minutes</span>
                  <Select
                    classNamePrefix="rs"
                    options={CLUTCH_MINUTES_OPTIONS}
                    value={CLUTCH_MINUTES_OPTIONS.find((opt) => opt.value === teamFilters.clutchMinutes) ?? null}
                    onChange={(selected) =>
                      setState((prev) => ({ ...prev, teams: { ...prev.teams, clutchMinutes: selected?.value ?? 5 } }))
                    }
                  />
                </label>
              </div>
              <label className="field">
                <span>Score Status</span>
                <select
                  value={teamFilters.clutchStatus}
                  onChange={(e) =>
                    setState((prev) => ({
                      ...prev,
                      teams: { ...prev.teams, clutchStatus: e.target.value as 'all' | 'leading' | 'trailing' | 'tied' },
                    }))
                  }
                >
                  <option value="all">All</option>
                  <option value="leading">Leading</option>
                  <option value="trailing">Trailing</option>
                  <option value="tied">Tied</option>
                </select>
              </label>
            </>
          ) : null}
        </details>
      ) : null}

      {tab === 'gamelogs' ? (
        <details className="filter-section">
          <summary>Game Logs Specific</summary>
          <label className="field">
            <span>Team (optional)</span>
            <Select
              classNamePrefix="rs"
              isClearable
              options={TEAM_OPTIONS}
              value={TEAM_OPTIONS.find((opt) => opt.value === gameLogFilters.team) ?? null}
              onChange={(selected) =>
                setState((prev) => ({
                  ...prev,
                  gamelogs: { ...prev.gamelogs, team: selected?.value ?? '' },
                }))
              }
            />
          </label>
        </details>
      ) : null}

      {tab === 'lineups' ? (
        <section className="filter-section filter-section-static">
          <h3 className="filter-static-title">Clutch Filter</h3>
          <label className="field checkbox-field">
            <input
              type="checkbox"
              checked={lineupFilters.clutchEnabled}
              onChange={(e) => setState((prev) => ({ ...prev, lineups: { ...prev.lineups, clutchEnabled: e.target.checked } }))}
            />
            <span>Enable clutch filter</span>
          </label>
          {lineupFilters.clutchEnabled ? (
            <>
              <div className="field-row">
                <label className="field">
                  <span>Max Margin</span>
                  <Select
                    classNamePrefix="rs"
                    options={CLUTCH_MARGIN_OPTIONS}
                    value={CLUTCH_MARGIN_OPTIONS.find((opt) => opt.value === lineupFilters.clutchMargin) ?? null}
                    onChange={(selected) =>
                      setState((prev) => ({
                        ...prev,
                        lineups: { ...prev.lineups, clutchMargin: selected?.value ?? 0 },
                      }))
                    }
                  />
                </label>
                <label className="field">
                  <span>Max Minutes</span>
                  <Select
                    classNamePrefix="rs"
                    options={CLUTCH_MINUTES_OPTIONS}
                    value={CLUTCH_MINUTES_OPTIONS.find((opt) => opt.value === lineupFilters.clutchMinutes) ?? null}
                    onChange={(selected) =>
                      setState((prev) => ({
                        ...prev,
                        lineups: { ...prev.lineups, clutchMinutes: selected?.value ?? 5 },
                      }))
                    }
                  />
                </label>
              </div>
              <label className="field">
                <span>Score Status</span>
                <select
                  value={lineupFilters.clutchStatus}
                  onChange={(e) =>
                    setState((prev) => ({
                      ...prev,
                      lineups: {
                        ...prev.lineups,
                        clutchStatus: e.target.value as 'all' | 'leading' | 'trailing' | 'tied',
                      },
                    }))
                  }
                >
                  <option value="all">All</option>
                  <option value="leading">Leading</option>
                  <option value="trailing">Trailing</option>
                  <option value="tied">Tied</option>
                </select>
              </label>
            </>
          ) : null}
        </section>
      ) : null}

    </section>
  );
}
