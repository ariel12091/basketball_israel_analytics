import { createContext, useContext } from 'react';
import type { FilterState } from '../../types';

/** Map game_year to season date bounds (mirrors Shiny's season_date_bounds) */
export function seasonDateBounds(gameYear: number): { start: string; end: string } {
  // Season runs from Oct 1 of (year-1) to Jul 1 of year
  return {
    start: `${gameYear - 1}-10-01`,
    end: `${gameYear}-07-01`,
  };
}

const defaultBounds = seasonDateBounds(2026);

export const DEFAULT_FILTERS: FilterState = {
  gameYear: 2026,
  startDate: defaultBounds.start,
  endDate: defaultBounds.end,
  teamIds: [],
  minOnPoss: 300,
  minAllPoss: 100,
  gameType: [],
  opponents: [],
  homeAway: '',
  outcome: '',
  gnMin: null,
  gnMax: null,
  lastN: null,
  oppRankSide: '',
  oppRankN: null,
  oppRankMetric: '',
  lineupPlayersActive: false,
  resetSeq: 0,
};

export type FilterAction =
  | { type: 'SET_FIELD'; field: keyof FilterState; value: FilterState[keyof FilterState] }
  | { type: 'RESET' }
  | { type: 'SET_MULTIPLE'; payload: Partial<FilterState> };

export function filterReducer(state: FilterState, action: FilterAction): FilterState {
  switch (action.type) {
    case 'SET_FIELD': {
      const next = { ...state, [action.field]: action.value };
      // When gameYear changes, reset dates + team/opponent filters (IDs are season-specific)
      if (action.field === 'gameYear') {
        const bounds = seasonDateBounds(action.value as number);
        next.startDate = bounds.start;
        next.endDate = bounds.end;
        next.teamIds = [];
        next.opponents = [];
      }
      // GN mutual exclusion: lastN vs gnMin/gnMax
      if (action.field === 'lastN' && action.value != null) {
        next.gnMin = null;
        next.gnMax = null;
      }
      if ((action.field === 'gnMin' || action.field === 'gnMax') && action.value != null) {
        next.lastN = null;
      }
      return next;
    }
    case 'SET_MULTIPLE':
      return { ...state, ...action.payload };
    case 'RESET': {
      // Reset to defaults but keep current gameYear's date bounds
      const bounds = seasonDateBounds(state.gameYear);
      return {
        ...DEFAULT_FILTERS,
        gameYear: state.gameYear,
        startDate: bounds.start,
        endDate: bounds.end,
        resetSeq: state.resetSeq + 1,
      };
    }
    default:
      return state;
  }
}

interface FilterContextValue {
  state: FilterState;
  dispatch: React.Dispatch<FilterAction>;
  drawerOpen: boolean;
  setDrawerOpen: (open: boolean) => void;
}

export const FilterContext = createContext<FilterContextValue>({
  state: DEFAULT_FILTERS,
  dispatch: () => {},
  drawerOpen: false,
  setDrawerOpen: () => {},
});

export function useFilters() {
  return useContext(FilterContext);
}

/** Returns true if any filter requires the SQL function path (not MV) */
export function needsFilteredPath(state: FilterState): boolean {
  const bounds = seasonDateBounds(state.gameYear);
  return (
    state.teamIds.length > 0 ||
    state.opponents.length > 0 ||
    state.gameType.length > 0 ||
    state.homeAway !== '' ||
    state.outcome !== '' ||
    state.gnMin !== null ||
    state.gnMax !== null ||
    state.lastN !== null ||
    state.oppRankSide !== '' ||
    state.startDate !== bounds.start ||
    state.endDate !== bounds.end
  );
}

/** Build API query params from filter state.
 *  team_ids, min_on, min_all are client-side only (applied on MV data locally). */
export function buildApiParams(state: FilterState): Record<string, unknown> {
  return {
    game_year: state.gameYear,
    start_date: state.startDate,
    end_date: state.endDate,
    game_type: state.gameType.length > 0 ? state.gameType.join(',') : undefined,
    opp_ids: state.opponents.length > 0 ? state.opponents.join(',') : undefined,
    home_away: state.homeAway || undefined,
    outcome: state.outcome || undefined,
    gn_min: state.gnMin ?? undefined,
    gn_max: state.gnMax ?? undefined,
    last_n: state.lastN ?? undefined,
    opp_rank_side: state.oppRankSide || undefined,
    opp_rank_n: state.oppRankN ?? undefined,
    opp_rank_metric: state.oppRankMetric || undefined,
  };
}
