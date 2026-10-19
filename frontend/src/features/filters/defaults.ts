import { BaseFilters, TabFilterState } from './types';

export const DEFAULT_BASE_FILTERS: BaseFilters = {
  season: '2026',
  dateStart: '2025-10-01',
  dateEnd: '2026-07-01',
  gameType: [''],
  opponents: [],
  homeAway: '',
  outcome: '',
  gnMin: '',
  gnMax: '',
  lastN: '',
  oppRankSide: '',
  oppRankN: '',
  oppRankMetric: '',
};

export function buildDefaultTabFilters(): TabFilterState {
  return {
    onoff: {
      ...DEFAULT_BASE_FILTERS,
      teams: [],
      minAllPoss: 200,
      minOnPoss: 300,
    },
    lineups: {
      ...DEFAULT_BASE_FILTERS,
      minPoss: 200,
      groupSize: '5',
      team: '',
      playersOn: [],
      playersOff: [],
      clutchEnabled: false,
      clutchMargin: 5,
      clutchStatus: 'all',
      clutchMinutes: 5,
      clutchOtMargin: false,
    },
    teams: {
      ...DEFAULT_BASE_FILTERS,
      clutchEnabled: false,
      clutchMargin: 5,
      clutchStatus: 'all',
      clutchMinutes: 5,
      clutchOtMargin: false,
    },
    gamelogs: {
      ...DEFAULT_BASE_FILTERS,
      team: '',
    },
  };
}
