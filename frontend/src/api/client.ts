import {
  GameLogFourFactorsRow,
  GameLogSummaryRow,
  LineupFourFactorsRow,
  LineupGameRow,
  LineupSummaryRow,
  OnOffFourFactorsRow,
  OnOffSummaryRow,
  QueryPayload,
  TableResponse,
  TeamFourFactorsRow,
  TeamSummaryRow,
} from './contracts';

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? '/api';

type FallbackRow =
  | OnOffSummaryRow
  | OnOffFourFactorsRow
  | LineupSummaryRow
  | LineupFourFactorsRow
  | LineupGameRow
  | TeamSummaryRow
  | TeamFourFactorsRow
  | GameLogSummaryRow
  | GameLogFourFactorsRow;

const mockOnOffSummary: OnOffSummaryRow[] = [
  { id: 'onoff-1', team: 'Hapoel Tel Aviv', player: 'Tyler Ennis', net: 29.5, off: 15.7, def: -13.8, onOffPpp: 129.4, onDefPpp: 93.2, onNetRtg: 36.2, offOffPpp: 113.7, offDefPpp: 107.0, offNetRtg: 6.7, offFg2Pct: 64, offFg3Pct: 36, offFg2Acc: 59, offFg3Acc: 38, defFg2Pct: 66, defFg3Pct: 34, defFg2Acc: 51, defFg3Acc: 29, onPoss: 446, offPoss: 786 },
  { id: 'onoff-2', team: 'Maccabi Tel Aviv', player: 'Jimmy Clark III', net: 26.6, off: 17.7, def: -8.9, onOffPpp: 135.4, onDefPpp: 102.7, onNetRtg: 32.7, offOffPpp: 115.2, offDefPpp: 109.8, offNetRtg: 5.4, offFg2Pct: 56, offFg3Pct: 44, offFg2Acc: 58, offFg3Acc: 42, defFg2Pct: 50, defFg3Pct: 50, defFg2Acc: 54, defFg3Acc: 35, onPoss: 632, offPoss: 658 },
  { id: 'onoff-3', team: 'Kiryat Ata', player: 'Darius Hannah', net: 23.5, off: 14.0, def: -9.5, onOffPpp: 112.5, onDefPpp: 106.6, onNetRtg: 5.9, offOffPpp: 102.4, offDefPpp: 112.1, offNetRtg: -9.7, offFg2Pct: 59, offFg3Pct: 41, offFg2Acc: 60, offFg3Acc: 36, defFg2Pct: 59, defFg3Pct: 41, defFg2Acc: 49, defFg3Acc: 33, onPoss: 633, offPoss: 676 },
  { id: 'onoff-4', team: 'Ness Ziona', player: 'Tyreek Smith', net: 23.4, off: 10.0, def: -13.4, onOffPpp: 109.7, onDefPpp: 99.3, onNetRtg: 10.4, offOffPpp: 101.0, offDefPpp: 112.5, offNetRtg: -11.5, offFg2Pct: 57, offFg3Pct: 43, offFg2Acc: 59, offFg3Acc: 26, defFg2Pct: 61, defFg3Pct: 39, defFg2Acc: 52, defFg3Acc: 29, onPoss: 304, offPoss: 869 },
  { id: 'onoff-5', team: 'Hapoel Holon', player: 'Marcus Bingham', net: 18.7, off: 9.8, def: -8.9, onOffPpp: 118.3, onDefPpp: 101.8, onNetRtg: 16.5, offOffPpp: 110.1, offDefPpp: 109.7, offNetRtg: 0.4, offFg2Pct: 61, offFg3Pct: 39, offFg2Acc: 56, offFg3Acc: 35, defFg2Pct: 64, defFg3Pct: 36, defFg2Acc: 53, defFg3Acc: 31 },
  { id: 'onoff-6', team: 'Bnei Herzliya', player: 'Chris Silva', net: 14.2, off: 6.1, def: -8.1, onOffPpp: 114.2, onDefPpp: 103.0, onNetRtg: 11.2, offOffPpp: 109.7, offDefPpp: 112.0, offNetRtg: -2.3, offFg2Pct: 63, offFg3Pct: 37, offFg2Acc: 55, offFg3Acc: 34, defFg2Pct: 62, defFg3Pct: 38, defFg2Acc: 52, defFg3Acc: 30 },
  { id: 'onoff-7', team: 'Hapoel Jerusalem', player: 'Noam Dovrat', net: 12.4, off: 7.0, def: -5.4, onOffPpp: 116.0, onDefPpp: 104.5, onNetRtg: 11.5, offOffPpp: 110.2, offDefPpp: 108.7, offNetRtg: 1.5, offFg2Pct: 60, offFg3Pct: 40, offFg2Acc: 57, offFg3Acc: 36, defFg2Pct: 65, defFg3Pct: 35, defFg2Acc: 53, defFg3Acc: 32 },
  { id: 'onoff-8', team: 'Maccabi Ramat Gan', player: 'Ronaldo Sagu', net: 11.8, off: 5.5, def: -6.3, onOffPpp: 113.5, onDefPpp: 102.0, onNetRtg: 11.5, offOffPpp: 108.1, offDefPpp: 109.7, offNetRtg: -1.6, offFg2Pct: 58, offFg3Pct: 42, offFg2Acc: 54, offFg3Acc: 33, defFg2Pct: 63, defFg3Pct: 37, defFg2Acc: 51, defFg3Acc: 30 },
  { id: 'onoff-9', team: 'Hapoel Haifa', player: 'Kendrick Ray', net: 9.2, off: 4.1, def: -5.1, onOffPpp: 111.8, onDefPpp: 103.4, onNetRtg: 8.4, offOffPpp: 106.4, offDefPpp: 110.1, offNetRtg: -3.7, offFg2Pct: 57, offFg3Pct: 43, offFg2Acc: 53, offFg3Acc: 32, defFg2Pct: 61, defFg3Pct: 39, defFg2Acc: 50, defFg3Acc: 29 },
  { id: 'onoff-10', team: 'Galil Elyon', player: 'Lior Eliyahu', net: 7.5, off: 3.3, def: -4.2, onOffPpp: 109.9, onDefPpp: 103.1, onNetRtg: 6.8, offOffPpp: 105.0, offDefPpp: 108.5, offNetRtg: -3.5, offFg2Pct: 56, offFg3Pct: 44, offFg2Acc: 52, offFg3Acc: 31, defFg2Pct: 60, defFg3Pct: 40, defFg2Acc: 49, defFg3Acc: 28 },
  { id: 'onoff-11', team: 'Elitzur Netanya', player: 'Amit Gershon', net: 6.9, off: 2.8, def: -4.1, onOffPpp: 108.7, onDefPpp: 102.9, onNetRtg: 5.8, offOffPpp: 104.4, offDefPpp: 108.2, offNetRtg: -3.8, offFg2Pct: 55, offFg3Pct: 45, offFg2Acc: 51, offFg3Acc: 31, defFg2Pct: 59, defFg3Pct: 41, defFg2Acc: 49, defFg3Acc: 28 },
  { id: 'onoff-12', team: 'Ironi Kiryat', player: 'Yotam Hanochi', net: 5.4, off: 2.0, def: -3.4, onOffPpp: 107.3, onDefPpp: 102.5, onNetRtg: 4.8, offOffPpp: 103.9, offDefPpp: 107.3, offNetRtg: -3.4, offFg2Pct: 55, offFg3Pct: 45, offFg2Acc: 50, offFg3Acc: 30, defFg2Pct: 58, defFg3Pct: 42, defFg2Acc: 48, defFg3Acc: 27 },
];

const mockOnOffFF: OnOffFourFactorsRow[] = [
  {
    id: 'onoffff-1', team: 'Kiryat Ata', player: 'Darius Hannah', offTsDiff: 9.1, offOrebDiff: 2.4, offTovDiff: -1.2, offFtrDiff: 4.6,
    defTsDiff: -4.1, defOrebDiff: -1.4, defTovDiff: 1.8, defFtrDiff: -2.1,
    offOnTs: 61.3, offOffTs: 52.2, offOnOreb: 30.2, offOffOreb: 27.8, offOnTov: 11.5, offOffTov: 12.7, offOnFtr: 35.1, offOffFtr: 30.5,
    defOnTs: 49.1, defOffTs: 53.2, defOnOreb: 23.4, defOffOreb: 24.8, defOnTov: 17.9, defOffTov: 16.1, defOnFtr: 23.8, defOffFtr: 25.9,
    offOnTsRank: 89, offOffTsRank: 46, offOnOrebRank: 83, offOffOrebRank: 51, offOnTovRank: 34, offOffTovRank: 58, offOnFtrRank: 86, offOffFtrRank: 48,
    defOnTsRank: 18, defOffTsRank: 63, defOnOrebRank: 31, defOffOrebRank: 57, defOnTovRank: 74, defOffTovRank: 44, defOnFtrRank: 27, defOffFtrRank: 61,
    prDiffOffTs: 0.89, prDiffOffOreb: 0.83, prDiffOffTov: 0.24, prDiffOffFtr: 0.86,
    prDiffDefTs: 0.17, prDiffDefOreb: 0.29, prDiffDefTov: 0.78, prDiffDefFtr: 0.23,
    onPoss: 633, offPoss: 676,
  },
  {
    id: 'onoffff-2', team: 'Hapoel Tel Aviv', player: 'Tyler Ennis', offTsDiff: 6.8, offOrebDiff: 1.5, offTovDiff: -0.6, offFtrDiff: 3.1,
    defTsDiff: -3.4, defOrebDiff: -0.9, defTovDiff: 1.2, defFtrDiff: -1.6,
    offOnTs: 60.4, offOffTs: 53.6, offOnOreb: 29.1, offOffOreb: 27.6, offOnTov: 11.2, offOffTov: 11.8, offOnFtr: 34.0, offOffFtr: 30.9,
    defOnTs: 50.2, defOffTs: 53.6, defOnOreb: 24.1, defOffOreb: 25.0, defOnTov: 17.2, defOffTov: 16.0, defOnFtr: 24.6, defOffFtr: 26.2,
    offOnTsRank: 85, offOffTsRank: 52, offOnOrebRank: 79, offOffOrebRank: 54, offOnTovRank: 39, offOffTovRank: 50, offOnFtrRank: 82, offOffFtrRank: 55,
    defOnTsRank: 24, defOffTsRank: 58, defOnOrebRank: 36, defOffOrebRank: 52, defOnTovRank: 68, defOffTovRank: 48, defOnFtrRank: 32, defOffFtrRank: 57,
    prDiffOffTs: 0.84, prDiffOffOreb: 0.77, prDiffOffTov: 0.31, prDiffOffFtr: 0.79,
    prDiffDefTs: 0.21, prDiffDefOreb: 0.35, prDiffDefTov: 0.71, prDiffDefFtr: 0.29,
    onPoss: 446, offPoss: 786,
  },
  {
    id: 'onoffff-3', team: 'Maccabi Tel Aviv', player: 'Jimmy Clark III', offTsDiff: 6.2, offOrebDiff: 1.1, offTovDiff: -0.9, offFtrDiff: 2.7,
    defTsDiff: -2.8, defOrebDiff: -1.1, defTovDiff: 1.4, defFtrDiff: -1.3,
    offOnTs: 61.8, offOffTs: 55.6, offOnOreb: 28.4, offOffOreb: 27.3, offOnTov: 10.6, offOffTov: 11.5, offOnFtr: 33.2, offOffFtr: 30.5,
    defOnTs: 51.4, defOffTs: 54.2, defOnOreb: 23.9, defOffOreb: 25.0, defOnTov: 17.3, defOffTov: 15.9, defOnFtr: 24.9, defOffFtr: 26.2,
    offOnTsRank: 82, offOffTsRank: 49, offOnOrebRank: 77, offOffOrebRank: 53, offOnTovRank: 31, offOffTovRank: 49, offOnFtrRank: 79, offOffFtrRank: 52,
    defOnTsRank: 28, defOffTsRank: 54, defOnOrebRank: 34, defOffOrebRank: 56, defOnTovRank: 70, defOffTovRank: 45, defOnFtrRank: 35, defOffFtrRank: 58,
    prDiffOffTs: 0.79, prDiffOffOreb: 0.73, prDiffOffTov: 0.27, prDiffOffFtr: 0.74,
    prDiffDefTs: 0.25, prDiffDefOreb: 0.31, prDiffDefTov: 0.74, prDiffDefFtr: 0.33,
    onPoss: 632, offPoss: 658,
  },
  {
    id: 'onoffff-4', team: 'Ness Ziona', player: 'Tyreek Smith', offTsDiff: 5.4, offOrebDiff: 1.9, offTovDiff: -0.4, offFtrDiff: 2.2,
    defTsDiff: -2.5, defOrebDiff: -0.8, defTovDiff: 1.0, defFtrDiff: -1.0,
    offOnTs: 58.9, offOffTs: 53.5, offOnOreb: 30.0, offOffOreb: 28.1, offOnTov: 11.7, offOffTov: 12.1, offOnFtr: 31.7, offOffFtr: 29.5,
    defOnTs: 50.5, defOffTs: 53.0, defOnOreb: 24.2, defOffOreb: 25.0, defOnTov: 16.9, defOffTov: 15.9, defOnFtr: 24.8, defOffFtr: 25.8,
    offOnTsRank: 80, offOffTsRank: 44, offOnOrebRank: 81, offOffOrebRank: 49, offOnTovRank: 42, offOffTovRank: 54, offOnFtrRank: 76, offOffFtrRank: 46,
    defOnTsRank: 31, defOffTsRank: 56, defOnOrebRank: 39, defOffOrebRank: 53, defOnTovRank: 66, defOffTovRank: 46, defOnFtrRank: 38, defOffFtrRank: 55,
    prDiffOffTs: 0.75, prDiffOffOreb: 0.79, prDiffOffTov: 0.39, prDiffOffFtr: 0.69,
    prDiffDefTs: 0.29, prDiffDefOreb: 0.37, prDiffDefTov: 0.67, prDiffDefFtr: 0.38,
    onPoss: 304, offPoss: 869,
  },
  {
    id: 'onoffff-5', team: 'Hapoel Holon', player: 'Marcus Bingham', offTsDiff: 4.2, offOrebDiff: 1.3, offTovDiff: -0.7, offFtrDiff: 2.0,
    defTsDiff: -2.1, defOrebDiff: -0.6, defTovDiff: 0.9, defFtrDiff: -0.8,
    offOnTs: 57.8, offOffTs: 53.6, offOnOreb: 28.8, offOffOreb: 27.5, offOnTov: 11.0, offOffTov: 11.7, offOnFtr: 32.5, offOffFtr: 30.5,
    defOnTs: 51.0, defOffTs: 53.1, defOnOreb: 24.0, defOffOreb: 24.6, defOnTov: 16.8, defOffTov: 15.9, defOnFtr: 24.7, defOffFtr: 25.5,
    offOnTsRank: 76, offOffTsRank: 50, offOnOrebRank: 72, offOffOrebRank: 52, offOnTovRank: 35, offOffTovRank: 52, offOnFtrRank: 71, offOffFtrRank: 50,
    defOnTsRank: 36, defOffTsRank: 55, defOnOrebRank: 42, defOffOrebRank: 51, defOnTovRank: 64, defOffTovRank: 47, defOnFtrRank: 41, defOffFtrRank: 54,
    prDiffOffTs: 0.68, prDiffOffOreb: 0.66, prDiffOffTov: 0.33, prDiffOffFtr: 0.64,
    prDiffDefTs: 0.34, prDiffDefOreb: 0.42, prDiffDefTov: 0.63, prDiffDefFtr: 0.41,
  },
  {
    id: 'onoffff-6', team: 'Hapoel Jerusalem', player: 'Noam Dovrat', offTsDiff: 3.6, offOrebDiff: 0.8, offTovDiff: -0.3, offFtrDiff: 1.6,
    defTsDiff: -1.9, defOrebDiff: -0.4, defTovDiff: 0.7, defFtrDiff: -0.6,
    offOnTs: 56.9, offOffTs: 53.3, offOnOreb: 28.1, offOffOreb: 27.3, offOnTov: 11.4, offOffTov: 11.7, offOnFtr: 31.4, offOffFtr: 29.8,
    defOnTs: 51.2, defOffTs: 53.1, defOnOreb: 24.1, defOffOreb: 24.5, defOnTov: 16.6, defOffTov: 15.9, defOnFtr: 24.4, defOffFtr: 25.0,
    offOnTsRank: 73, offOffTsRank: 54, offOnOrebRank: 69, offOffOrebRank: 55, offOnTovRank: 44, offOffTovRank: 53, offOnFtrRank: 67, offOffFtrRank: 54,
    defOnTsRank: 39, defOffTsRank: 53, defOnOrebRank: 45, defOffOrebRank: 50, defOnTovRank: 61, defOffTovRank: 49, defOnFtrRank: 43, defOffFtrRank: 53,
    prDiffOffTs: 0.63, prDiffOffOreb: 0.59, prDiffOffTov: 0.45, prDiffOffFtr: 0.61,
    prDiffDefTs: 0.39, prDiffDefOreb: 0.46, prDiffDefTov: 0.59, prDiffDefFtr: 0.44,
  },
];

const mockLineupSummary: LineupSummaryRow[] = [
  {
    id: 'lineup-hash-1',
    team: 'Maccabi Ramat Gan',
    players: 'Ronaldo Sagu, Adam Ariel, Drew Crawford, Joirdon Nichols, Darion Atkins',
    offPpp: 119.3,
    defPpp: 97.8,
    netRtg: 21.5,
    plusMinus: 78,
    min: 93.7,
    totalPoss: 364,
    offPoss: 182,
    offPts: 217,
    defPoss: 182,
    defPts: 178,
    offFg2Pct: 62,
    offFg3Pct: 38,
    offFg2Acc: 57,
    offFg3Acc: 37,
    defFg2Pct: 64,
    defFg3Pct: 36,
    defFg2Acc: 51,
    defFg3Acc: 31,
  },
  {
    id: 'lineup-hash-2',
    team: 'Hapoel Tel Aviv',
    players: 'Tyler Ennis, Guy Palatin, John Doe, Example Wing, Example Big',
    offPpp: 122.0,
    defPpp: 101.1,
    netRtg: 20.9,
    plusMinus: 64,
    min: 88.1,
    totalPoss: 331,
    offPoss: 165,
    offPts: 201,
    defPoss: 166,
    defPts: 168,
    offFg2Pct: 60,
    offFg3Pct: 40,
    offFg2Acc: 58,
    offFg3Acc: 36,
    defFg2Pct: 63,
    defFg3Pct: 37,
    defFg2Acc: 52,
    defFg3Acc: 30,
  },
];

const mockLineupFF: LineupFourFactorsRow[] = [
  {
    id: 'lineup-hash-1',
    team: 'Maccabi Ramat Gan',
    players: 'Ronaldo Sagu, Adam Ariel, Drew Crawford, Joirdon Nichols, Darion Atkins',
    tsOff: 58.7,
    tsDef: 50.9,
    orebOff: 29.2,
    orebDef: 24.0,
    tovOff: 13.6,
    tovDef: 18.1,
    ftrOff: 33.4,
    ftrDef: 23.5,
    netRtg: 21.5,
    onPctRank: 84,
    offPctRank: 48,
    min: 93.7,
    totalPoss: 364,
  },
  {
    id: 'lineup-hash-2',
    team: 'Hapoel Tel Aviv',
    players: 'Tyler Ennis, Guy Palatin, John Doe, Example Wing, Example Big',
    tsOff: 60.1,
    tsDef: 52.4,
    orebOff: 31.0,
    orebDef: 25.2,
    tovOff: 12.9,
    tovDef: 17.4,
    ftrOff: 34.7,
    ftrDef: 24.8,
    netRtg: 20.9,
    onPctRank: 81,
    offPctRank: 50,
    min: 88.1,
    totalPoss: 331,
  },
];

const mockLineupGames: Record<string, LineupGameRow[]> = {
  'lineup-hash-1': [
    { id: 'lg-1', gn: 11, date: '2025-11-15', opponent: 'Hapoel Holon', score: '114-60', result: 'W' },
    { id: 'lg-2', gn: 12, date: '2025-11-20', opponent: 'Maccabi Tel Aviv', score: '96-99', result: 'L' },
  ],
  'lineup-hash-2': [
    { id: 'lg-3', gn: 14, date: '2025-12-01', opponent: 'Bnei Herzliya', score: '103-89', result: 'W' },
  ],
};

function toResponse<T extends FallbackRow>(rows: T[]): TableResponse<T> {
  return { rows, total: rows.length };
}

function includesIgnoreCase(value: string, query: string): boolean {
  return value.toLowerCase().includes(query.toLowerCase());
}

function applyTeamLikeFilter<T extends { team?: string }>(rows: T[], teams?: string[]): T[] {
  if (!teams || teams.length === 0) return rows;
  return rows.filter((row) => teams.some((t) => row.team && includesIgnoreCase(row.team, t)));
}

async function postJson<TResponse>(path: string, payload: QueryPayload): Promise<TResponse> {
  try {
    const response = await fetch(`${API_BASE}${path}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`API ${response.status}: ${text}`);
    }

    return response.json() as Promise<TResponse>;
  } catch (error) {
    return getFallbackResponse(path, payload, error) as TResponse;
  }
}

function getFallbackResponse(path: string, payload: QueryPayload, error: unknown): TableResponse<FallbackRow> {
  console.warn(`[apiClient] Falling back to mock data for ${path}`, error);
  const extra = payload.extra ?? {};

  if (path === '/onoff/summary') {
    const rows = applyTeamLikeFilter(mockOnOffSummary, (extra.teams as string[] | undefined) ?? []);
    return toResponse(rows);
  }
  if (path === '/onoff/four-factors') {
    const rows = applyTeamLikeFilter(mockOnOffFF, (extra.teams as string[] | undefined) ?? []);
    return toResponse(rows);
  }
  if (path === '/lineups/summary') {
    let rows = mockLineupSummary;
    const team = (extra.team as string | undefined) ?? '';
    if (team) rows = rows.filter((r) => includesIgnoreCase(r.team, team));
    const playersOn = (extra.playersOn as string[] | undefined) ?? [];
    if (playersOn.length) {
      rows = rows.filter((r) => playersOn.every((p) => includesIgnoreCase(r.players, p)));
    }
    const playersOff = (extra.playersOff as string[] | undefined) ?? [];
    if (playersOff.length) {
      rows = rows.filter((r) => playersOff.every((p) => !includesIgnoreCase(r.players, p)));
    }
    return toResponse(rows);
  }
  if (path === '/lineups/four-factors') {
    let rows = mockLineupFF;
    const team = (extra.team as string | undefined) ?? '';
    if (team) rows = rows.filter((r) => includesIgnoreCase(r.team, team));
    const playersOn = (extra.playersOn as string[] | undefined) ?? [];
    if (playersOn.length) {
      rows = rows.filter((r) => playersOn.every((p) => includesIgnoreCase(r.players, p)));
    }
    const playersOff = (extra.playersOff as string[] | undefined) ?? [];
    if (playersOff.length) {
      rows = rows.filter((r) => playersOff.every((p) => !includesIgnoreCase(r.players, p)));
    }
    return toResponse(rows);
  }
  if (path.startsWith('/lineups/') && path.endsWith('/games')) {
    const hash = decodeURIComponent(path.split('/')[2] ?? '');
    return toResponse(mockLineupGames[hash] ?? []);
  }

  return toResponse([]);
}

export const apiClient = {
  onoffSummary: (payload: QueryPayload) =>
    postJson<TableResponse<OnOffSummaryRow>>('/onoff/summary', payload),
  onoffFourFactors: (payload: QueryPayload) =>
    postJson<TableResponse<OnOffFourFactorsRow>>('/onoff/four-factors', payload),

  lineupSummary: (payload: QueryPayload) =>
    postJson<TableResponse<LineupSummaryRow>>('/lineups/summary', payload),
  lineupFourFactors: (payload: QueryPayload) =>
    postJson<TableResponse<LineupFourFactorsRow>>('/lineups/four-factors', payload),
  lineupGames: (hash: string, payload: QueryPayload) =>
    postJson<TableResponse<LineupGameRow>>(`/lineups/${encodeURIComponent(hash)}/games`, payload),

  teamSummary: (payload: QueryPayload) =>
    postJson<TableResponse<TeamSummaryRow>>('/teams/summary', payload),
  teamFourFactors: (payload: QueryPayload) =>
    postJson<TableResponse<TeamFourFactorsRow>>('/teams/four-factors', payload),

  gameLogsSummary: (payload: QueryPayload) =>
    postJson<TableResponse<GameLogSummaryRow>>('/gamelogs/summary', payload),
  gameLogsFourFactors: (payload: QueryPayload) =>
    postJson<TableResponse<GameLogFourFactorsRow>>('/gamelogs/four-factors', payload),
};
