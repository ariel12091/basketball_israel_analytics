// Tab 1 Summary — from onoff_compute() / onoff_default_mv
// Column names map to the SQL quoted names like "Net RTG Diff" etc.
export interface OnOffPlayer {
  team: string;
  firstName: string;
  lastName: string;
  netDiff: number;
  offOnDiff: number;
  defOnDiff: number;
  offOnPpp: number;
  defOnPpp: number;
  onNetRtg: number;
  offOffPpp: number;
  defOffPpp: number;
  offNetRtg: number;
  onPoss: number;
  offPoss: number;
  prNet: number;       // Net RTG Diff background
  prOffOn: number;     // Off ON PPP background
  prOffOff: number;    // Off OFF PPP background
  prDefOnInv: number;  // Def ON PPP background (inverted)
  prDefOffInv: number; // Def OFF PPP background (inverted)
  prOffOnD: number;    // Off ON Diff background
  prDefOnD: number;    // Def ON Diff background (NOT inverted — use COLS_REV)
  prOnNet: number;     // On Net RTG background
  prOffNet: number;    // Off Net RTG background
  // Shot splits (16 cols: off/def x on/off x fg2/fg3 x made/att)
  offOnFg2Made: number;
  offOnFg2Att: number;
  offOnFg3Made: number;
  offOnFg3Att: number;
  offOffFg2Made: number;
  offOffFg2Att: number;
  offOffFg3Made: number;
  offOffFg3Att: number;
  defOnFg2Made: number;
  defOnFg2Att: number;
  defOnFg3Made: number;
  defOnFg3Att: number;
  defOffFg2Made: number;
  defOffFg2Att: number;
  defOffFg3Made: number;
  defOffFg3Att: number;
  playerId: number;
  teamId: number;
}

// Tab 1 Four Factors — from four_factors_compute() / player_advanced_stats_mv
export interface OnOffFourFactors {
  playerId: number;
  teamId: number;
  firstName: string;
  lastName: string;
  teamName: string;
  // On/off rates
  offOnTs: number;
  offOffTs: number;
  defOnTs: number;
  defOffTs: number;
  offOnOreb: number;
  offOffOreb: number;
  defOnOreb: number;
  defOffOreb: number;
  offOnTov: number;
  offOffTov: number;
  defOnTov: number;
  defOffTov: number;
  offOnFtr: number;
  offOffFtr: number;
  defOnFtr: number;
  defOffFtr: number;
  offOnPoss: number;
  offOffPoss: number;
  defOnPoss: number;
  defOffPoss: number;
  // Diffs
  offTsDiff: number;
  offOrebDiff: number;
  offTovDiff: number;
  offFtrDiff: number;
  defTsDiff: number;
  defOrebDiff: number;
  defTovDiff: number;
  defFtrDiff: number;
  // From onoff join
  netRtgDiff: number;
  offDiff: number;
  defDiff: number;
}

export interface FilterState {
  gameYear: number;
  startDate: string;
  endDate: string;
  teamIds: number[];
  minOnPoss: number;
  minAllPoss: number;
  gameType: number[];
  opponents: number[];
  homeAway: string;
  outcome: string;
  gnMin: number | null;
  gnMax: number | null;
  lastN: number | null;
  oppRankSide: string;   // '' | 'top' | 'bottom'
  oppRankN: number | null;    // 1-12
  oppRankMetric: string; // '' | 'off' | 'def' | 'net'
  lineupPlayersActive: boolean;
  resetSeq: number;
}

export interface Team {
  teamId: number;
  teamName: string;
}

export interface Player {
  playerId: number;
  teamId: number;
  name: string;
}

// Tab 2 Summary — from fetch_lineups_csv_v2
export interface LineupSummary {
  teamId: number;
  subLineupHash: string;
  numLineup: number;
  playerIds: number[];
  playerNamesStr: string;
  offPoss: number;
  offPts: number;
  offPpp: number;
  defPoss: number;
  defPts: number;
  defPpp: number;
  netRtg: number;
  minutes: number;
  totalPoss: number;
  plusMinus: number;
  // Shot splits (8 cols: off/def x fg2/fg3 x made/att)
  offFg2Made: number;
  offFg2Att: number;
  offFg3Made: number;
  offFg3Att: number;
  defFg2Made: number;
  defFg2Att: number;
  defFg3Made: number;
  defFg3Att: number;
  // Percentile ranks (computed server-side)
  prNet: number | null;
  prOffPpp: number | null;
  prDefPppInv: number | null;
  // Flag for TOTAL row
  isTotal?: boolean;
}

// Tab 2 Four Factors — from fetch_lineups_four_factors_csv
export interface LineupFourFactors {
  teamId: number;
  subLineupHash: string;
  numLineup: number;
  playerIds: number[];
  playerNamesStr: string;
  offTs: number;
  offOreb: number;
  offTov: number;
  offFtr: number;
  offPoss: number;
  offPts: number;
  offPpp: number;
  defTs: number;
  defOreb: number;
  defTov: number;
  defFtr: number;
  defPoss: number;
  defPts: number;
  defPpp: number;
  netRtg: number;
  minutes: number;
  totalPoss: number;
  // Raw counts for TOTAL row aggregation
  offTsPoss: number;
  offOrebCnt: number;
  offOrebOpps: number;
  offTovCnt: number;
  offFta: number;
  offFgaCnt: number;
  defTsPoss: number;
  defOrebCnt: number;
  defOrebOpps: number;
  defTovCnt: number;
  defFta: number;
  defFgaCnt: number;
  // Percentile ranks (computed server-side)
  prNet: number | null;
  prOffPpp: number | null;
  prOffTs: number | null;
  prOffOreb: number | null;
  prOffTov: number | null;
  prOffFtr: number | null;
  prDefPpp: number | null;
  prDefTs: number | null;
  prDefOreb: number | null;
  prDefTov: number | null;
  prDefFtr: number | null;
  // Flag for TOTAL row
  isTotal?: boolean;
}

// Wrapper for lineup API responses (server-side ranking)
export interface LineupApiResponse<T> {
  rows: T[];
  meta: {
    autoMinPoss: number;
  };
}

// Lineup game log (modal)
export interface LineupGameLog {
  gn: number;
  gameDate: string;
  opponent: string;
  result: string;
  score: string;
  offPpp: number;
  defPpp: number;
  netRtg: number;
  offPoss: number;
  defPoss: number;
  minutes: number;
  // Summary: shot splits
  offFg2Made?: number;
  offFg2Att?: number;
  offFg3Made?: number;
  offFg3Att?: number;
  defFg2Made?: number;
  defFg2Att?: number;
  defFg3Made?: number;
  defFg3Att?: number;
  // FF rates
  offTs?: number;
  offOreb?: number;
  offTov?: number;
  offFtr?: number;
  defTs?: number;
  defOreb?: number;
  defTov?: number;
  defFtr?: number;
}

export type SortDir = 'asc' | 'desc';

export interface ColumnDef<T> {
  key: string;
  header: string;
  tip?: string;
  group?: string;
  sectionStart?: boolean;
  sortable?: boolean;
  render?: (row: T, index: number) => React.ReactNode;
  className?: string;
}
