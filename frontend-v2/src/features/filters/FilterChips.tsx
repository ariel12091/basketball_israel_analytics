import { useFilters, DEFAULT_FILTERS, needsFilteredPath, seasonDateBounds } from './store';

function seasonLabel(year: number): string {
  return `${year - 1}-${String(year).slice(2)}`;
}

export default function FilterChips() {
  const { state, dispatch } = useFilters();
  const chips: { label: string; removable: boolean; onRemove?: () => void }[] = [];

  // Season (always shown, not removable)
  chips.push({ label: seasonLabel(state.gameYear), removable: false });

  // Game type
  if (state.gameType.length > 0) {
    const typeMap: Record<number, string> = { 5: 'Regular', 16: 'PO QF', 26: 'PO SF', 17: 'PO Finals', 33: 'Play-in', 34: 'Winner Cup', 35: 'State Cup' };
    const label = state.gameType.map(t => typeMap[t] ?? `Type ${t}`).join(', ');
    chips.push({
      label,
      removable: true,
      onRemove: () => dispatch({ type: 'SET_FIELD', field: 'gameType', value: [] }),
    });
  }

  // Team filter
  if (state.teamIds.length > 0) {
    const label = state.teamIds.length === 1 ? `1 team` : `${state.teamIds.length} teams`;
    chips.push({
      label,
      removable: true,
      onRemove: () => dispatch({ type: 'SET_FIELD', field: 'teamIds', value: [] }),
    });
  }

  // Opponents
  if (state.opponents.length > 0) {
    const label = state.opponents.length === 1 ? `vs 1 opp` : `vs ${state.opponents.length} opps`;
    chips.push({
      label,
      removable: true,
      onRemove: () => dispatch({ type: 'SET_FIELD', field: 'opponents', value: [] }),
    });
  }

  // Min ON poss (if not default)
  if (state.minOnPoss > 0 && state.minOnPoss !== DEFAULT_FILTERS.minOnPoss) {
    chips.push({
      label: `Min ${state.minOnPoss} ON Poss`,
      removable: true,
      onRemove: () =>
        dispatch({ type: 'SET_FIELD', field: 'minOnPoss', value: DEFAULT_FILTERS.minOnPoss }),
    });
  }

  // Date range (if not default for current season)
  const bounds = seasonDateBounds(state.gameYear);
  if (state.startDate !== bounds.start || state.endDate !== bounds.end) {
    chips.push({
      label: `${state.startDate} to ${state.endDate}`,
      removable: true,
      onRemove: () =>
        dispatch({
          type: 'SET_MULTIPLE',
          payload: { startDate: bounds.start, endDate: bounds.end },
        }),
    });
  }

  // Home/Away
  if (state.homeAway) {
    chips.push({
      label: state.homeAway,
      removable: true,
      onRemove: () => dispatch({ type: 'SET_FIELD', field: 'homeAway', value: '' }),
    });
  }

  // Outcome
  if (state.outcome) {
    chips.push({
      label: state.outcome,
      removable: true,
      onRemove: () => dispatch({ type: 'SET_FIELD', field: 'outcome', value: '' }),
    });
  }

  // GN range
  if (state.gnMin !== null || state.gnMax !== null) {
    const from = state.gnMin !== null ? `GN ${state.gnMin}` : '';
    const to = state.gnMax !== null ? `GN ${state.gnMax}` : '';
    const label = from && to ? `${from}–${to}` : from || to;
    chips.push({
      label,
      removable: true,
      onRemove: () =>
        dispatch({ type: 'SET_MULTIPLE', payload: { gnMin: null, gnMax: null } }),
    });
  }

  // Opponent Strength
  if (state.oppRankSide) {
    const metricLabel = state.oppRankMetric === 'off' ? 'Off' : state.oppRankMetric === 'def' ? 'Def' : state.oppRankMetric === 'net' ? 'Net' : '';
    const nLabel = state.oppRankN ? ` ${state.oppRankN}` : '';
    chips.push({
      label: `vs ${state.oppRankSide}${nLabel} ${metricLabel}`.trim(),
      removable: true,
      onRemove: () =>
        dispatch({
          type: 'SET_MULTIPLE',
          payload: { oppRankSide: '', oppRankN: null, oppRankMetric: '' },
        }),
    });
  }

  // Last N
  if (state.lastN) {
    chips.push({
      label: `Last ${state.lastN} games`,
      removable: true,
      onRemove: () => dispatch({ type: 'SET_FIELD', field: 'lastN', value: null }),
    });
  }

  const hasActiveFilters = needsFilteredPath(state) ||
    state.minOnPoss !== DEFAULT_FILTERS.minOnPoss ||
    state.minAllPoss !== DEFAULT_FILTERS.minAllPoss ||
    state.lineupPlayersActive;

  if (chips.length === 0 && !hasActiveFilters) return null;

  return (
    <div className="filter-chips">
      {chips.map((c, i) => (
        <span className="chip" key={i}>
          {c.label}
          {c.removable && (
            <button className="chip-remove" onClick={c.onRemove}>
              &times;
            </button>
          )}
        </span>
      ))}
      {hasActiveFilters && (
        <button
          className="chip chip-clear"
          onClick={() => dispatch({ type: 'RESET' })}
        >
          Clear all
        </button>
      )}
    </div>
  );
}
