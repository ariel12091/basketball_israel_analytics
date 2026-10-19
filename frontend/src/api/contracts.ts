export interface BaseTableRow {
  id: string;
}

export interface OnOffSummaryRow extends BaseTableRow {
  team: string;
  player: string;
  net: number;
  off: number;
  def: number;
  onOffPpp?: number;
  onDefPpp?: number;
  onNetRtg?: number;
  offOffPpp?: number;
  offDefPpp?: number;
  offNetRtg?: number;
  offFg2Pct?: number;
  offFg3Pct?: number;
  offFg2Acc?: number;
  offFg3Acc?: number;
  defFg2Pct?: number;
  defFg3Pct?: number;
  defFg2Acc?: number;
  defFg3Acc?: number;
  onPoss?: number;
  offPoss?: number;
}

export interface OnOffFourFactorsRow extends BaseTableRow {
  team: string;
  player: string;
  tsDiff?: number;
  orebDiff?: number;
  tovDiff?: number;
  ftrDiff?: number;
  netDiff?: number;
  offRtgDiff?: number;
  defRtgDiff?: number;
  offTsDiff?: number;
  offOrebDiff?: number;
  offTovDiff?: number;
  offFtrDiff?: number;
  defTsDiff?: number;
  defOrebDiff?: number;
  defTovDiff?: number;
  defFtrDiff?: number;
  offOnTs?: number;
  offOffTs?: number;
  offOnOreb?: number;
  offOffOreb?: number;
  offOnTov?: number;
  offOffTov?: number;
  offOnFtr?: number;
  offOffFtr?: number;
  defOnTs?: number;
  defOffTs?: number;
  defOnOreb?: number;
  defOffOreb?: number;
  defOnTov?: number;
  defOffTov?: number;
  defOnFtr?: number;
  defOffFtr?: number;
  offOnTsRank?: number;
  offOffTsRank?: number;
  offOnOrebRank?: number;
  offOffOrebRank?: number;
  offOnTovRank?: number;
  offOffTovRank?: number;
  offOnFtrRank?: number;
  offOffFtrRank?: number;
  defOnTsRank?: number;
  defOffTsRank?: number;
  defOnOrebRank?: number;
  defOffOrebRank?: number;
  defOnTovRank?: number;
  defOffTovRank?: number;
  defOnFtrRank?: number;
  defOffFtrRank?: number;
  onPoss?: number;
  offPoss?: number;
  onPctRank?: number;
  offPctRank?: number;
  [key: string]: number | string | undefined;
}

export interface LineupSummaryRow extends BaseTableRow {
  team: string;
  players: string;
  offPpp: number;
  defPpp: number;
  netRtg: number;
  plusMinus?: number;
  min?: number;
  totalPoss?: number;
  offPoss?: number;
  offPts?: number;
  defPoss?: number;
  defPts?: number;
  offFg2Pct?: number;
  offFg3Pct?: number;
  offFg2Acc?: number;
  offFg3Acc?: number;
  defFg2Pct?: number;
  defFg3Pct?: number;
  defFg2Acc?: number;
  defFg3Acc?: number;
}

export interface LineupFourFactorsRow extends BaseTableRow {
  team: string;
  players: string;
  tsOff: number;
  tsDef: number;
  orebOff?: number;
  orebDef?: number;
  tovOff?: number;
  tovDef?: number;
  ftrOff?: number;
  ftrDef?: number;
  netRtg: number;
  onPctRank?: number;
  offPctRank?: number;
  min?: number;
  totalPoss?: number;
}

export interface TeamSummaryRow extends BaseTableRow {
  team: string;
  offPpp: number;
  defPpp: number;
  netRtg: number;
  games: number;
  wl: string;
}

export interface TeamFourFactorsRow extends BaseTableRow {
  team: string;
  tsOff: number;
  tsDef: number;
  netRtg: number;
}

export interface GameLogSummaryRow extends BaseTableRow {
  gn: number;
  team: string;
  opp: string;
  score: string;
  offPpp: number;
  defPpp: number;
  netRtg: number;
}

export interface GameLogFourFactorsRow extends BaseTableRow {
  gn: number;
  team: string;
  opp: string;
  tsOff: number;
  tsDef: number;
  netRtg: number;
}

export interface LineupGameRow extends BaseTableRow {
  gn: number;
  date: string;
  opponent: string;
  score: string;
  result: 'W' | 'L';
}

export interface TableResponse<T> {
  rows: T[];
  total: number;
}

export interface ApiError {
  code: string;
  message: string;
}

export interface ApiMeta {
  lastUpdated: string;
}

export interface QueryPayload {
  filters: object;
  mode?: 'Summary' | 'Four Factors';
  page?: number;
  pageSize?: number;
  extra?: Record<string, unknown>;
}
