export type AppRouteTab = 'onoff' | 'lineups' | 'teams' | 'gamelogs';

export interface BaseFilters {
  season: '2026' | '2025';
  dateStart: string;
  dateEnd: string;
  gameType: string[];
  opponents: string[];
  homeAway: '' | 'home' | 'away';
  outcome: '' | 'win' | 'loss';
  gnMin: string;
  gnMax: string;
  lastN: string;
  oppRankSide: '' | 'top' | 'bottom';
  oppRankN: string;
  oppRankMetric: '' | 'off' | 'def' | 'net';
}

export interface OnOffFilters extends BaseFilters {
  teams: string[];
  minAllPoss: number;
  minOnPoss: number;
}

export interface LineupFilters extends BaseFilters {
  minPoss: number;
  groupSize: '2' | '3' | '4' | '5';
  team: string;
  playersOn: string[];
  playersOff: string[];
  clutchEnabled: boolean;
  clutchMargin: number;
  clutchStatus: 'all' | 'leading' | 'trailing' | 'tied';
  clutchMinutes: number;
  clutchOtMargin: boolean;
}

export interface TeamFilters extends BaseFilters {
  clutchEnabled: boolean;
  clutchMargin: number;
  clutchStatus: 'all' | 'leading' | 'trailing' | 'tied';
  clutchMinutes: number;
  clutchOtMargin: boolean;
}

export interface GameLogFilters extends BaseFilters {
  team: string;
}

export interface TabFilterState {
  onoff: OnOffFilters;
  lineups: LineupFilters;
  teams: TeamFilters;
  gamelogs: GameLogFilters;
}
