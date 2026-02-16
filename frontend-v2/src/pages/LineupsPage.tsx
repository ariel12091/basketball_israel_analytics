import { useState, useMemo, useCallback, useEffect, useRef } from 'react';
import { useFilters, buildApiParams } from '../features/filters/store';
import { useApi } from '../hooks/useApi';
import { useSorting } from '../hooks/useSorting';
import DataTable, { exportCSV } from '../features/tables/DataTable';
import type { ColumnGroup, Column } from '../features/tables/DataTable';
import HeatCell from '../features/tables/HeatCell';
import ShotCell from '../features/tables/ShotCell';
import LineupModal from '../features/tables/LineupModal';
import type { LineupSummary, LineupFourFactors, Team, Player } from '../types';
import {
  autoMinPoss,
  adaptiveBaseline,
  percentileRank,
  computeShotAvgs,
  SHOT_MIN_FGA,
} from '../utils/ranking';

type ViewMode = 'summary' | 'ff';
type GroupSize = 2 | 3 | 4 | 5;

// ─── LineupsPage ────────────────────────────────────────────
export default function LineupsPage() {
  const { state: filters } = useFilters();

  // Tab-2 local state (not in shared FilterContext)
  const [mode, setMode] = useState<ViewMode>('summary');
  const [groupSize, setGroupSize] = useState<GroupSize>(5);
  const [teamId, setTeamId] = useState<number | null>(null);
  const [playersOn, setPlayersOn] = useState<number[]>([]);
  const [playersOff, setPlayersOff] = useState<number[]>([]);
  const [minPoss, setMinPoss] = useState(20);
  const [explainerOpen, setExplainerOpen] = useState(false);

  // Clutch state
  const [clutchEnabled, setClutchEnabled] = useState(false);
  const [clutchMargin, setClutchMargin] = useState(5);
  const [clutchStatus, setClutchStatus] = useState('all');
  const [clutchMinutes, setClutchMinutes] = useState(5);
  const [clutchOtMargin, setClutchOtMargin] = useState(false);

  // Auto min-poss
  const autoEnabled = useRef(true);
  const autoUpdating = useRef(false);
  const prevMinPoss = useRef(minPoss);

  // Modal state
  const [modalHash, setModalHash] = useState<string | null>(null);
  const [modalTeamId, setModalTeamId] = useState<number>(0);

  // Build API params: shared filters + Tab 2-specific (groupSize, clutch, GN)
  // NOTE: team/player/minPoss are client-side-only filters
  const apiParams = useMemo(() => {
    const base = buildApiParams(filters);
    const p: Record<string, unknown> = {
      ...base,
      num: groupSize,
    };
    if (clutchEnabled) {
      p.clutch_margin = clutchMargin;
      p.clutch_status = clutchStatus;
      p.clutch_minutes = clutchMinutes;
      if (clutchOtMargin) p.clutch_ot_margin = 'true';
    }
    return p;
  }, [filters, groupSize, clutchEnabled, clutchMargin, clutchStatus, clutchMinutes, clutchOtMargin]);

  // Fetch data
  const { data: summaryRaw, loading: summaryLoading, error: summaryError } = useApi<LineupSummary[]>(
    '/api/lineups/summary', apiParams, mode === 'summary',
  );
  const { data: ffRaw, loading: ffLoading, error: ffError } = useApi<LineupFourFactors[]>(
    '/api/lineups/four-factors', apiParams, mode === 'ff',
  );

  // Fetch teams and players for filters
  const teamsParams = useMemo(() => ({ game_year: filters.gameYear }), [filters.gameYear]);
  const { data: teams } = useApi<Team[]>('/api/meta/teams', teamsParams);
  const { data: allPlayers } = useApi<Player[]>('/api/meta/players', teamsParams);

  // Filter player choices by selected team
  const teamPlayers = useMemo(() => {
    if (!allPlayers || !teamId) return [];
    return allPlayers.filter(p => p.teamId === teamId);
  }, [allPlayers, teamId]);
  const teamNameById = useMemo(
    () => new Map((teams ?? []).map(t => [t.teamId, t.teamName])),
    [teams],
  );

  // When team changes, clear player selections
  useEffect(() => {
    setPlayersOn([]);
    setPlayersOff([]);
  }, [teamId]);

  // ─── Auto min-poss ────────────────────────────────────────
  useEffect(() => {
    if (minPoss !== prevMinPoss.current) {
      if (!autoUpdating.current) autoEnabled.current = false;
      autoUpdating.current = false;
      prevMinPoss.current = minPoss;
    }
  }, [minPoss]);

  // Re-enable auto when filters change
  useEffect(() => {
    autoEnabled.current = true;
  }, [apiParams, teamId, playersOn, playersOff]);

  // Compute auto threshold
  useEffect(() => {
    if (!autoEnabled.current) return;
    const raw = mode === 'summary' ? summaryRaw : ffRaw;
    if (!raw?.length) return;
    // Apply local filters first (team + players), then compute threshold
    let data = raw as Array<{ totalPoss: number; teamId: number; playerIds: number[] }>;
    if (teamId) data = data.filter(d => d.teamId === teamId);
    if (playersOn.length) data = data.filter(d => playersOn.every(id => d.playerIds.includes(id)));
    if (playersOff.length) data = data.filter(d => !playersOff.some(id => d.playerIds.includes(id)));

    const needed = autoMinPoss(data.map(d => ({ poss: d.totalPoss })));
    if (minPoss > needed) {
      autoUpdating.current = true;
      setMinPoss(needed);
    }
  }, [summaryRaw, ffRaw, mode, teamId, playersOn, playersOff, minPoss]);

  // ─── Summary: rank + filter + TOTAL ────────────────────────
  const summaryRanked = useMemo(() => {
    if (!summaryRaw?.length) return [];
    const possVec = summaryRaw.map(d => d.totalPoss);
    const thresh = adaptiveBaseline(possVec);
    const qualify = (vals: number[]) => vals.map((v, i) => possVec[i] >= thresh ? v : null);

    const prNet = percentileRank(qualify(summaryRaw.map(d => d.netRtg)));
    const prOffPpp = percentileRank(qualify(summaryRaw.map(d => d.offPpp)));
    const prDefPpp = percentileRank(qualify(summaryRaw.map(d => d.defPpp)));

    return summaryRaw.map((d, i) => ({
      ...d,
      prNet: prNet[i],
      prOffPpp: prOffPpp[i],
      prDefPppInv: prDefPpp[i] === null ? null : 1 - prDefPpp[i]!,
    }));
  }, [summaryRaw]);

  const summaryFiltered = useMemo(() => {
    let data = summaryRanked;
    if (teamId) data = data.filter(d => d.teamId === teamId);
    if (playersOn.length) data = data.filter(d => playersOn.every(id => d.playerIds.includes(id)));
    if (playersOff.length) data = data.filter(d => !playersOff.some(id => d.playerIds.includes(id)));
    return data.filter(d => d.totalPoss >= minPoss);
  }, [summaryRanked, teamId, playersOn, playersOff, minPoss]);

  // TOTAL row for summary
  const summaryWithTotal = useMemo(() => {
    if (!summaryFiltered.length) return [];
    const sumOffPts = summaryFiltered.reduce((s, l) => s + l.offPts, 0);
    const sumOffPoss = summaryFiltered.reduce((s, l) => s + l.offPoss, 0);
    const sumDefPts = summaryFiltered.reduce((s, l) => s + l.defPts, 0);
    const sumDefPoss = summaryFiltered.reduce((s, l) => s + l.defPoss, 0);
    const sumMins = summaryFiltered.reduce((s, l) => s + l.minutes, 0);
    const totOffPpp = sumOffPoss > 0 ? (sumOffPts / sumOffPoss) * 100 : 0;
    const totDefPpp = sumDefPoss > 0 ? (sumDefPts / sumDefPoss) * 100 : 0;

    const total: LineupSummary = {
      teamId: 0, subLineupHash: 'TOTAL', numLineup: 0,
      playerIds: [], playerNamesStr: '— All Lineups —',
      offPoss: sumOffPoss, offPts: sumOffPts, offPpp: totOffPpp,
      defPoss: sumDefPoss, defPts: sumDefPts, defPpp: totDefPpp,
      netRtg: totOffPpp - totDefPpp, minutes: sumMins,
      totalPoss: sumOffPoss + sumDefPoss,
      plusMinus: sumOffPts - sumDefPts,
      offFg2Made: summaryFiltered.reduce((s, l) => s + l.offFg2Made, 0),
      offFg2Att: summaryFiltered.reduce((s, l) => s + l.offFg2Att, 0),
      offFg3Made: summaryFiltered.reduce((s, l) => s + l.offFg3Made, 0),
      offFg3Att: summaryFiltered.reduce((s, l) => s + l.offFg3Att, 0),
      defFg2Made: summaryFiltered.reduce((s, l) => s + l.defFg2Made, 0),
      defFg2Att: summaryFiltered.reduce((s, l) => s + l.defFg2Att, 0),
      defFg3Made: summaryFiltered.reduce((s, l) => s + l.defFg3Made, 0),
      defFg3Att: summaryFiltered.reduce((s, l) => s + l.defFg3Att, 0),
      isTotal: true,
    };
    return [total, ...summaryFiltered];
  }, [summaryFiltered]);

  // ─── FF: rank + filter + TOTAL ─────────────────────────────
  const ffRanked = useMemo(() => {
    if (!ffRaw?.length) return [];
    const possVec = ffRaw.map(d => d.totalPoss);
    const thresh = adaptiveBaseline(possVec);
    const qualify = (vals: number[]) => vals.map((v, i) => possVec[i] >= thresh ? v : null);

    const prNet = percentileRank(qualify(ffRaw.map(d => d.netRtg)));
    const prOffPpp = percentileRank(qualify(ffRaw.map(d => d.offPpp)));
    const prOffTs = percentileRank(qualify(ffRaw.map(d => d.offTs)));
    const prOffOreb = percentileRank(qualify(ffRaw.map(d => d.offOreb)));
    const prOffTov = percentileRank(qualify(ffRaw.map(d => d.offTov)));
    const prOffFtr = percentileRank(qualify(ffRaw.map(d => d.offFtr)));
    const prDefPpp = percentileRank(qualify(ffRaw.map(d => d.defPpp)));
    const prDefTs = percentileRank(qualify(ffRaw.map(d => d.defTs)));
    const prDefOreb = percentileRank(qualify(ffRaw.map(d => d.defOreb)));
    const prDefTov = percentileRank(qualify(ffRaw.map(d => d.defTov)));
    const prDefFtr = percentileRank(qualify(ffRaw.map(d => d.defFtr)));

    return ffRaw.map((d, i) => ({
      ...d,
      prNet: prNet[i],
      prOffPpp: prOffPpp[i],
      prOffTs: prOffTs[i],
      prOffOreb: prOffOreb[i],
      prOffTov: prOffTov[i] === null ? null : 1 - prOffTov[i]!,  // inverted
      prOffFtr: prOffFtr[i],
      prDefPpp: prDefPpp[i] === null ? null : 1 - prDefPpp[i]!,  // inverted
      prDefTs: prDefTs[i] === null ? null : 1 - prDefTs[i]!,     // inverted
      prDefOreb: prDefOreb[i] === null ? null : 1 - prDefOreb[i]!,// inverted
      prDefTov: prDefTov[i],  // NOT inverted (opponent TOV = good)
      prDefFtr: prDefFtr[i] === null ? null : 1 - prDefFtr[i]!,  // inverted
    }));
  }, [ffRaw]);

  const ffFiltered = useMemo(() => {
    let data = ffRanked;
    if (teamId) data = data.filter(d => d.teamId === teamId);
    if (playersOn.length) data = data.filter(d => playersOn.every(id => d.playerIds.includes(id)));
    if (playersOff.length) data = data.filter(d => !playersOff.some(id => d.playerIds.includes(id)));
    return data.filter(d => d.totalPoss >= minPoss);
  }, [ffRanked, teamId, playersOn, playersOff, minPoss]);

  const ffWithTotal = useMemo(() => {
    if (!ffFiltered.length) return [];
    const sumOffPts = ffFiltered.reduce((s, l) => s + l.offPts, 0);
    const sumOffPoss = ffFiltered.reduce((s, l) => s + l.offPoss, 0);
    const sumDefPts = ffFiltered.reduce((s, l) => s + l.defPts, 0);
    const sumDefPoss = ffFiltered.reduce((s, l) => s + l.defPoss, 0);
    const sumMins = ffFiltered.reduce((s, l) => s + l.minutes, 0);
    const totOffPpp = sumOffPoss > 0 ? (sumOffPts / sumOffPoss) * 100 : 0;
    const totDefPpp = sumDefPoss > 0 ? (sumDefPts / sumDefPoss) * 100 : 0;

    // Sum raw counts for FF rates
    const sOffTsPoss = ffFiltered.reduce((s, l) => s + l.offTsPoss, 0);
    const sOffOrebCnt = ffFiltered.reduce((s, l) => s + l.offOrebCnt, 0);
    const sOffOrebOpps = ffFiltered.reduce((s, l) => s + l.offOrebOpps, 0);
    const sOffTovCnt = ffFiltered.reduce((s, l) => s + l.offTovCnt, 0);
    const sOffFta = ffFiltered.reduce((s, l) => s + l.offFta, 0);
    const sOffFga = ffFiltered.reduce((s, l) => s + l.offFgaCnt, 0);
    const sDefTsPoss = ffFiltered.reduce((s, l) => s + l.defTsPoss, 0);
    const sDefOrebCnt = ffFiltered.reduce((s, l) => s + l.defOrebCnt, 0);
    const sDefOrebOpps = ffFiltered.reduce((s, l) => s + l.defOrebOpps, 0);
    const sDefTovCnt = ffFiltered.reduce((s, l) => s + l.defTovCnt, 0);
    const sDefFta = ffFiltered.reduce((s, l) => s + l.defFta, 0);
    const sDefFga = ffFiltered.reduce((s, l) => s + l.defFgaCnt, 0);

    const total: LineupFourFactors = {
      teamId: 0, subLineupHash: 'TOTAL', numLineup: 0,
      playerIds: [], playerNamesStr: '— All Lineups —',
      offTs: sOffTsPoss > 0 ? (sumOffPts / (2 * sOffTsPoss)) * 100 : 0,
      offOreb: sOffOrebOpps > 0 ? (sOffOrebCnt / sOffOrebOpps) * 100 : 0,
      offTov: sumOffPoss > 0 ? (sOffTovCnt / sumOffPoss) * 100 : 0,
      offFtr: sOffFga > 0 ? (sOffFta / sOffFga) * 100 : 0,
      offPoss: sumOffPoss, offPts: sumOffPts, offPpp: totOffPpp,
      defTs: sDefTsPoss > 0 ? (sumDefPts / (2 * sDefTsPoss)) * 100 : 0,
      defOreb: sDefOrebOpps > 0 ? (sDefOrebCnt / sDefOrebOpps) * 100 : 0,
      defTov: sumDefPoss > 0 ? (sDefTovCnt / sumDefPoss) * 100 : 0,
      defFtr: sDefFga > 0 ? (sDefFta / sDefFga) * 100 : 0,
      defPoss: sumDefPoss, defPts: sumDefPts, defPpp: totDefPpp,
      netRtg: totOffPpp - totDefPpp, minutes: sumMins,
      totalPoss: sumOffPoss + sumDefPoss,
      offTsPoss: sOffTsPoss, offOrebCnt: sOffOrebCnt, offOrebOpps: sOffOrebOpps,
      offTovCnt: sOffTovCnt, offFta: sOffFta, offFgaCnt: sOffFga,
      defTsPoss: sDefTsPoss, defOrebCnt: sDefOrebCnt, defOrebOpps: sDefOrebOpps,
      defTovCnt: sDefTovCnt, defFta: sDefFta, defFgaCnt: sDefFga,
      isTotal: true,
    };
    return [total, ...ffFiltered];
  }, [ffFiltered]);

  // ─── Sorting (pin TOTAL row at top) ──────────────────────
  const {
    sorted: summarySortedRaw,
    sortKey: sSortKey,
    sortDir: sSortDir,
    onSort: sOnSort,
  } = useSorting(summaryFiltered, 'totalPoss', 'desc');

  const summarySorted = useMemo(() => {
    if (!summaryWithTotal.length) return [];
    const total = summaryWithTotal[0]; // always the TOTAL row
    return [total, ...summarySortedRaw];
  }, [summaryWithTotal, summarySortedRaw]);

  const {
    sorted: ffSortedRaw,
    sortKey: fSortKey,
    sortDir: fSortDir,
    onSort: fOnSort,
  } = useSorting(ffFiltered, 'totalPoss', 'desc');

  const ffSorted = useMemo(() => {
    if (!ffWithTotal.length) return [];
    const total = ffWithTotal[0];
    return [total, ...ffSortedRaw];
  }, [ffWithTotal, ffSortedRaw]);

  // ─── Shot averages (summary) ──────────────────────────────
  const summaryColumns = useMemo(() => {
    const raw = summaryRaw ?? [];
    return buildSummaryColumns(
      computeShotAvgs(raw, 'offFg2Made', 'offFg2Att', 'offFg3Made', 'offFg3Att', SHOT_MIN_FGA),
      computeShotAvgs(raw, 'defFg2Made', 'defFg2Att', 'defFg3Made', 'defFg3Att', SHOT_MIN_FGA),
      teamNameById,
      (row: LineupSummary) => {
        if (row.isTotal) return;
        setModalHash(row.subLineupHash);
        setModalTeamId(row.teamId);
      },
    );
  }, [summaryRaw, teamNameById]);

  const ffColumnsBuilt = useMemo(() => buildFFColumns(
    teamNameById,
    (row: LineupFourFactors) => {
      if (row.isTotal) return;
      setModalHash(row.subLineupHash);
      setModalTeamId(row.teamId);
    },
  ), [teamNameById]);

  // ─── CSV export ───────────────────────────────────────────
  const handleSummaryExport = useCallback(() => {
    const keys = ['playerNamesStr', 'offPpp', 'defPpp', 'netRtg', 'offPoss', 'defPoss',
      'minutes', 'totalPoss', 'plusMinus',
      'offFg2Made', 'offFg2Att', 'offFg3Made', 'offFg3Att',
      'defFg2Made', 'defFg2Att', 'defFg3Made', 'defFg3Att'];
    const headers = ['Players', 'Off PPP', 'Def PPP', 'Net RTG', 'Off Poss', 'Def Poss',
      'Min', 'Total Poss', '+/-',
      'Off FG2 Made', 'Off FG2 Att', 'Off FG3 Made', 'Off FG3 Att',
      'Def FG2 Made', 'Def FG2 Att', 'Def FG3 Made', 'Def FG3 Att'];
    exportCSV(summarySorted, 'lineup_summary.csv', keys, headers);
  }, [summarySorted]);

  const handleFFExport = useCallback(() => {
    const keys = ['playerNamesStr', 'offPpp', 'offTs', 'offOreb', 'offTov', 'offFtr', 'offPoss',
      'defPpp', 'defTs', 'defOreb', 'defTov', 'defFtr', 'defPoss',
      'minutes', 'totalPoss', 'netRtg'];
    const headers = ['Players', 'Off PPP', 'Off TS%', 'Off OREB%', 'Off TOV%', 'Off FTR', 'Off Poss',
      'Def PPP', 'Def TS%', 'Def OREB%', 'Def TOV%', 'Def FTR', 'Def Poss',
      'Min', 'Total Poss', 'Net RTG'];
    exportCSV(ffSorted, 'lineup_four_factors.csv', keys, headers);
  }, [ffSorted]);

  const loading = mode === 'summary' ? summaryLoading : ffLoading;
  const error = mode === 'summary' ? summaryError : ffError;

  // ─── Mutual exclusion for Players On/Off ──────────────────
  const handlePlayerOnChange = (ids: number[]) => {
    setPlayersOn(ids);
    setPlayersOff(prev => prev.filter(id => !ids.includes(id)));
  };
  const handlePlayerOffChange = (ids: number[]) => {
    setPlayersOff(ids);
    setPlayersOn(prev => prev.filter(id => !ids.includes(id)));
  };

  return (
    <div className="panel-lineups">
      {/* Header */}
      <div className="section-header">
        <div className="section-title-area">
          <h2 className="section-title">Lineup Data</h2>
          <span className="section-subtitle">How do lineup combinations perform together?</span>
        </div>
        <div className="mode-toggle">
          <button className={`mode-btn ${mode === 'summary' ? 'active' : ''}`} onClick={() => setMode('summary')}>Summary</button>
          <button className={`mode-btn ${mode === 'ff' ? 'active' : ''}`} onClick={() => setMode('ff')}>Four Factors</button>
        </div>
      </div>

      {/* Explainer */}
      <div className={`explainer-bar ${explainerOpen ? 'open' : ''}`} onClick={() => setExplainerOpen(!explainerOpen)}>
        <div className="explainer-bar-left">
          <div className="explainer-icon">?</div>
          <span className="explainer-bar-title">How to read this table</span>
        </div>
        <span className="explainer-chevron">&#9660;</span>
      </div>
      {explainerOpen && (
        <div className="explainer-body show">
          {mode === 'summary' ? (
            <>
              <p style={{ marginBottom: 8 }}>
                <strong>Lineup combos</strong> show how groups of players perform together.
                Each row represents a unique player combination that appeared on the floor.
              </p>
              <p style={{ marginBottom: 8 }}>
                <strong>Net RTG</strong> = Offense PPP - Defense PPP. Positive = outscoring opponents.
                <strong> +/-</strong> = raw point differential (Off Pts - Def Pts).
              </p>
              <p>
                <strong>Tip:</strong> Use the Group Size pills (2-5) to see smaller combos.
                Click any lineup name to see its per-game breakdown.
              </p>
            </>
          ) : (
            <>
              <p style={{ marginBottom: 8 }}>
                <strong>Four Factors</strong> for lineup combos: TS%, OREB%, TOV%, FTR — same metrics as Tab 1,
                but for lineup groups instead of individual players.
              </p>
              <p>
                Background color indicates percentile rank among all lineups. Click any lineup name for per-game detail.
              </p>
            </>
          )}
        </div>
      )}

      {/* Tab 2 Controls */}
      <div className="lineup-controls">
        {/* Group Size */}
        <div className="group-size-bar">
          <span className="group-size-label">Group Size</span>
          {([2, 3, 4, 5] as GroupSize[]).map(n => (
            <button
              key={n}
              className={`group-pill ${groupSize === n ? 'active' : ''}`}
              onClick={() => setGroupSize(n)}
            >
              {n}
            </button>
          ))}
        </div>

        {/* Team + Players */}
        <div className="lineup-filter-row">
          <div className="lineup-filter-item">
            <label className="lineup-filter-label">Team</label>
            <select
              className="lineup-select"
              value={teamId ?? ''}
              onChange={e => setTeamId(e.target.value ? Number(e.target.value) : null)}
            >
              <option value="">All teams</option>
              {(teams ?? []).map(t => (
                <option key={t.teamId} value={t.teamId}>{t.teamName}</option>
              ))}
            </select>
          </div>
          <div className="lineup-filter-item">
            <label className="lineup-filter-label">Players On</label>
            <MultiSelect
              options={teamPlayers.filter(p => !playersOff.includes(p.playerId))}
              selected={playersOn}
              onChange={handlePlayerOnChange}
              placeholder={teamId ? 'Must include...' : 'Please select a team'}
              disabled={!teamId}
            />
          </div>
          <div className="lineup-filter-item">
            <label className="lineup-filter-label">Players Off</label>
            <MultiSelect
              options={teamPlayers.filter(p => !playersOn.includes(p.playerId))}
              selected={playersOff}
              onChange={handlePlayerOffChange}
              placeholder={teamId ? 'Must exclude...' : 'Please select a team'}
              disabled={!teamId}
            />
          </div>
        </div>

        {/* Clutch + Min Poss */}
        <div className="lineup-filter-row">
          <div className="lineup-filter-item">
            <label className="lineup-filter-label" style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <input
                type="checkbox"
                checked={clutchEnabled}
                onChange={e => setClutchEnabled(e.target.checked)}
              />
              Clutch Time
            </label>
            {clutchEnabled && (
              <div className="clutch-controls">
                <div className="clutch-row">
                  <span>Margin ≤ {clutchMargin}</span>
                  <input type="range" min={0} max={10} value={clutchMargin}
                    onChange={e => setClutchMargin(Number(e.target.value))} />
                </div>
                <div className="clutch-row">
                  <span>Status</span>
                  <select className="lineup-select" value={clutchStatus}
                    onChange={e => setClutchStatus(e.target.value)}>
                    <option value="all">All</option>
                    <option value="leading">Leading</option>
                    <option value="trailing">Trailing</option>
                    <option value="tied">Tied</option>
                  </select>
                </div>
                <div className="clutch-row">
                  <span>≤ {clutchMinutes} min left</span>
                  <input type="range" min={1} max={5} value={clutchMinutes}
                    onChange={e => setClutchMinutes(Number(e.target.value))} />
                </div>
                <div className="clutch-row">
                  <label style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 11 }}>
                    <input type="checkbox" checked={clutchOtMargin}
                      onChange={e => setClutchOtMargin(e.target.checked)} />
                    Apply margin to OT
                  </label>
                </div>
              </div>
            )}
          </div>
          <div className="lineup-filter-item">
            <label className="lineup-filter-label">Min Poss: {minPoss}</label>
            <input
              type="range"
              min={0} max={2000} step={10}
              value={minPoss}
              onChange={e => setMinPoss(Number(e.target.value))}
              className="lineup-slider"
            />
          </div>
        </div>
      </div>

      {/* Legend */}
      {mode === 'summary' && (
        <div className="legend-row">
          <span style={{ fontWeight: 600, color: 'var(--text-secondary)' }}>Shot Splits:</span>
          <div className="legend-item">
            <div className="legend-swatch" style={{ background: 'var(--fg2)' }} />
            <span>2PT freq</span>
          </div>
          <div className="legend-item">
            <div className="legend-swatch" style={{ background: 'var(--fg3)' }} />
            <span>3PT freq</span>
          </div>
          <span style={{ color: 'var(--text-muted)' }}>|</span>
          <div className="legend-item">
            <span style={{ color: 'var(--negative)', fontWeight: 600 }}>FG%</span>
            <span style={{ color: 'var(--text-muted)' }}>&rarr;</span>
            <span style={{ color: 'var(--positive)', fontWeight: 600 }}>FG%</span>
            <span style={{ marginLeft: 2 }}>(accuracy)</span>
          </div>
        </div>
      )}

      {/* Loading / Error / Empty */}
      {loading && (
        <div className="table-card">
          {[...Array(8)].map((_, i) => (
            <div className="skeleton-row" key={i}>
              <div className="skeleton-cell" style={{ width: 180 }} />
              <div className="skeleton-cell" style={{ width: 60 }} />
              <div className="skeleton-cell" style={{ width: 60 }} />
              <div className="skeleton-cell" style={{ width: 60 }} />
              <div className="skeleton-cell" style={{ width: 90 }} />
              <div className="skeleton-cell" style={{ width: 60 }} />
            </div>
          ))}
        </div>
      )}
      {error && !loading && (
        <div className="table-card" style={{ padding: '32px 0', textAlign: 'center' }}>
          <p style={{ color: 'var(--negative)', fontWeight: 600 }}>Failed to load data</p>
          <p style={{ color: 'var(--text-muted)', fontSize: 12, marginTop: 4 }}>{error}</p>
        </div>
      )}
      {!loading && !error && (
        (mode === 'summary' && summarySorted.length === 0 && summaryRaw !== null) ||
        (mode === 'ff' && ffSorted.length === 0 && ffRaw !== null)
      ) && (
        <div className="table-card" style={{ padding: '32px 0', textAlign: 'center' }}>
          <p style={{ color: 'var(--text-muted)' }}>
            No lineups match the current filters. Try reducing possession minimums or changing group size.
          </p>
        </div>
      )}

      {/* Summary Table */}
      {mode === 'summary' && !loading && summarySorted.length > 0 && (
        <DataTable<LineupSummary>
          groups={SUMMARY_GROUPS}
          columns={summaryColumns}
          data={summarySorted}
          sortKey={sSortKey}
          sortDir={sSortDir}
          onSort={sOnSort}
          infoText={`${summarySorted.length - 1} lineups · ${groupSize}-man combos`}
          onExport={handleSummaryExport}
          rowKey={(r) => r.subLineupHash}
        />
      )}

      {/* FF Table */}
      {mode === 'ff' && !loading && ffSorted.length > 0 && (
        <DataTable<LineupFourFactors>
          groups={FF_GROUPS}
          columns={ffColumnsBuilt}
          data={ffSorted}
          sortKey={fSortKey}
          sortDir={fSortDir}
          onSort={fOnSort}
          infoText={`${ffSorted.length - 1} lineups · ${groupSize}-man combos`}
          onExport={handleFFExport}
          rowKey={(r) => r.subLineupHash}
        />
      )}

      {/* Lineup Modal */}
      {modalHash && (
        <LineupModal
          subHash={modalHash}
          teamId={modalTeamId}
          gameYear={filters.gameYear}
          viewMode={mode}
          onClose={() => setModalHash(null)}
        />
      )}
    </div>
  );
}

// ─── Simple MultiSelect (no react-select dependency) ────────
function MultiSelect({
  options,
  selected,
  onChange,
  placeholder,
  disabled = false,
}: {
  options: Player[];
  selected: number[];
  onChange: (ids: number[]) => void;
  placeholder: string;
  disabled?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState('');
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  useEffect(() => {
    if (disabled) {
      setOpen(false);
      setSearch('');
    }
  }, [disabled]);

  const toggle = (id: number) => {
    if (disabled) return;
    onChange(selected.includes(id) ? selected.filter(x => x !== id) : [...selected, id]);
  };

  const selectedNames = options.filter(p => selected.includes(p.playerId)).map(p => p.name);
  const filteredOptions = options.filter(p =>
    p.name.toLowerCase().includes(search.trim().toLowerCase())
  );
  const displayText = selectedNames.length ? selectedNames.join(', ') : '';

  return (
    <div className="multi-select" ref={ref}>
      <div
        className="multi-select-display"
        onClick={() => {
          if (disabled) return;
          setOpen(true);
        }}
      >
        <input
          type="text"
          className="multi-select-input"
          value={search}
          onChange={e => {
            setSearch(e.target.value);
            if (!disabled) setOpen(true);
          }}
          onFocus={() => {
            if (!disabled) setOpen(true);
          }}
          placeholder={placeholder}
          disabled={disabled}
        />
        {!search && displayText && <span className="multi-select-value">{displayText}</span>}
      </div>
      {open && (
        <div className="multi-select-dropdown">
          {filteredOptions.map(p => (
            <label key={p.playerId} className="multi-select-option">
              <input
                type="checkbox"
                checked={selected.includes(p.playerId)}
                onChange={() => toggle(p.playerId)}
              />
              {p.name}
            </label>
          ))}
          {filteredOptions.length === 0 && (
            <div style={{ padding: 8, color: 'var(--text-muted)', fontSize: 11 }}>
              {options.length ? 'No matching players' : 'No players'}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ─── Column definitions ─────────────────────────────────────

const SUMMARY_GROUPS: ColumnGroup[] = [
  { label: '', span: 1, empty: true },
  { label: 'PERFORMANCE', span: 3, sectionStart: true },
  { label: 'OFF SHOT', span: 1, sectionStart: true },
  { label: 'DEF SHOT', span: 1 },
  { label: 'USAGE', span: 4, sectionStart: true },
];

function buildSummaryColumns(
  offAvg: { avg2: number; avg3: number },
  defAvg: { avg2: number; avg3: number },
  teamNameById: Map<number, string>,
  onClick: (row: LineupSummary) => void,
): Column<LineupSummary>[] {
  return [
    {
      key: 'playerNamesStr',
      header: 'Lineup',
      sortable: true,
      render: (row) => (
        <td key="players" style={{ textAlign: 'left', fontFamily: 'var(--font-sans)', minWidth: 160 }}>
          {row.isTotal ? (
            <span style={{ fontWeight: 700 }}>{row.playerNamesStr}</span>
          ) : (
            <a className="lineup-link" onClick={() => onClick(row)}>
              <div className="lineup-name">{formatLineupNames(row.playerNamesStr)}</div>
              <div className="lineup-team">{formatTeamName(teamNameById.get(row.teamId) ?? '')}</div>
            </a>
          )}
        </td>
      ),
    },
    {
      key: 'offPpp',
      header: 'Off PPP',
      tip: 'Offensive points per 100 possessions',
      sectionStart: true,
      render: (row) => row.isTotal ? (
        <td key="offPpp" className="section-start total-cell">{row.offPpp.toFixed(1)}</td>
      ) : (
        <HeatCell key="offPpp" value={row.offPpp} pr={row.prOffPpp ?? 0.5} format="ppp" sectionStart />
      ),
    },
    {
      key: 'defPpp',
      header: 'Def PPP',
      tip: 'Defensive points per 100 possessions',
      render: (row) => row.isTotal ? (
        <td key="defPpp" className="total-cell">{row.defPpp.toFixed(1)}</td>
      ) : (
        <HeatCell key="defPpp" value={row.defPpp} pr={row.prDefPppInv ?? 0.5} format="ppp" />
      ),
    },
    {
      key: 'netRtg',
      header: 'Net RTG',
      tip: 'Net rating (Off - Def PPP)',
      render: (row) => row.isTotal ? (
        <td key="net" className="total-cell">{fmtNet(row.netRtg)}</td>
      ) : (
        <HeatCell key="net" value={row.netRtg} pr={row.prNet ?? 0.5} format="net" bold />
      ),
    },
    {
      key: 'offShot',
      header: 'Off Shot',
      tip: 'Offensive 2PT/3PT splits',
      sortable: false,
      sectionStart: true,
      render: (row) => (
        <ShotCell
          key="offShot"
          fg2Made={row.offFg2Made} fg2Att={row.offFg2Att}
          fg3Made={row.offFg3Made} fg3Att={row.offFg3Att}
          avg2={offAvg.avg2} avg3={offAvg.avg3}
          sectionStart
        />
      ),
    },
    {
      key: 'defShot',
      header: 'Def Shot',
      tip: 'Defensive 2PT/3PT splits',
      sortable: false,
      render: (row) => (
        <ShotCell
          key="defShot"
          fg2Made={row.defFg2Made} fg2Att={row.defFg2Att}
          fg3Made={row.defFg3Made} fg3Att={row.defFg3Att}
          avg2={defAvg.avg2} avg3={defAvg.avg3}
          isDefense
        />
      ),
    },
    {
      key: 'offPoss',
      header: 'Off Poss',
      tip: 'Offensive possessions',
      sectionStart: true,
      render: (row) => (
        <td key="offPoss" className={`section-start ${row.isTotal ? 'total-cell' : ''}`}
          style={{ color: 'var(--text-secondary)' }}>
          {row.offPoss.toLocaleString()}
        </td>
      ),
    },
    {
      key: 'minutes',
      header: 'Min',
      tip: 'Minutes played',
      render: (row) => (
        <td key="min" className={row.isTotal ? 'total-cell' : ''} style={{ color: 'var(--text-muted)' }}>
          {row.minutes.toFixed(1)}
        </td>
      ),
    },
    {
      key: 'totalPoss',
      header: 'Poss',
      tip: 'Total possessions (Off + Def)',
      render: (row) => (
        <td key="totPoss" className={row.isTotal ? 'total-cell' : ''} style={{ color: 'var(--text-secondary)' }}>
          {row.totalPoss.toLocaleString()}
        </td>
      ),
    },
    {
      key: 'plusMinus',
      header: '+/-',
      tip: 'Raw point differential',
      render: (row) => (
        <td key="pm" className={row.isTotal ? 'total-cell' : ''}>
          {fmtPM(row.plusMinus)}
        </td>
      ),
    },
  ];
}

const FF_GROUPS: ColumnGroup[] = [
  { label: '', span: 1, empty: true },
  { label: 'OFFENSE', span: 6, sectionStart: true },
  { label: 'DEFENSE', span: 6, sectionStart: true },
  { label: 'USAGE', span: 3, sectionStart: true },
];

function buildFFColumns(
  teamNameById: Map<number, string>,
  onClick: (row: LineupFourFactors) => void,
): Column<LineupFourFactors>[] {
  return [
    {
      key: 'playerNamesStr',
      header: 'Lineup',
      sortable: true,
      render: (row) => (
        <td key="players" style={{ textAlign: 'left', fontFamily: 'var(--font-sans)', minWidth: 160 }}>
          {row.isTotal ? (
            <span style={{ fontWeight: 700 }}>{row.playerNamesStr}</span>
          ) : (
            <a className="lineup-link" onClick={() => onClick(row)}>
              <div className="lineup-name">{formatLineupNames(row.playerNamesStr)}</div>
              <div className="lineup-team">{formatTeamName(teamNameById.get(row.teamId) ?? '')}</div>
            </a>
          )}
        </td>
      ),
    },
    {
      key: 'offPpp',
      header: 'PPP',
      tip: 'Offensive PPP',
      sectionStart: true,
      render: (row) => row.isTotal ? (
        <td key="offPpp" className="section-start total-cell">{row.offPpp.toFixed(1)}</td>
      ) : (
        <HeatCell key="offPpp" value={row.offPpp} pr={row.prOffPpp ?? 0.5} format="ppp" sectionStart />
      ),
    },
    {
      key: 'offTs',
      header: 'TS%',
      tip: 'True Shooting %',
      render: (row) => row.isTotal ? (
        <td key="offTs" className="total-cell">{row.offTs.toFixed(1)}</td>
      ) : (
        <HeatCell key="offTs" value={row.offTs} pr={row.prOffTs ?? 0.5} format="ppp" />
      ),
    },
    {
      key: 'offOreb',
      header: 'OREB%',
      tip: 'Offensive rebound rate',
      render: (row) => row.isTotal ? (
        <td key="offOreb" className="total-cell">{row.offOreb.toFixed(1)}</td>
      ) : (
        <HeatCell key="offOreb" value={row.offOreb} pr={row.prOffOreb ?? 0.5} format="ppp" />
      ),
    },
    {
      key: 'offTov',
      header: 'TOV%',
      tip: 'Turnover rate',
      render: (row) => row.isTotal ? (
        <td key="offTov" className="total-cell">{row.offTov.toFixed(1)}</td>
      ) : (
        <HeatCell key="offTov" value={row.offTov} pr={row.prOffTov ?? 0.5} format="ppp" />
      ),
    },
    {
      key: 'offFtr',
      header: 'FTR',
      tip: 'Free throw rate',
      render: (row) => row.isTotal ? (
        <td key="offFtr" className="total-cell">{row.offFtr.toFixed(1)}</td>
      ) : (
        <HeatCell key="offFtr" value={row.offFtr} pr={row.prOffFtr ?? 0.5} format="ppp" />
      ),
    },
    {
      key: 'offPoss',
      header: 'Poss',
      tip: 'Offensive possessions',
      render: (row) => (
        <td key="offPoss" className={row.isTotal ? 'total-cell' : ''} style={{ color: 'var(--text-secondary)' }}>
          {row.offPoss.toLocaleString()}
        </td>
      ),
    },
    {
      key: 'defPpp',
      header: 'PPP',
      tip: 'Defensive PPP',
      sectionStart: true,
      render: (row) => row.isTotal ? (
        <td key="defPpp" className="section-start total-cell">{row.defPpp.toFixed(1)}</td>
      ) : (
        <HeatCell key="defPpp" value={row.defPpp} pr={row.prDefPpp ?? 0.5} format="ppp" sectionStart />
      ),
    },
    {
      key: 'defTs',
      header: 'TS%',
      tip: 'Opponent TS%',
      render: (row) => row.isTotal ? (
        <td key="defTs" className="total-cell">{row.defTs.toFixed(1)}</td>
      ) : (
        <HeatCell key="defTs" value={row.defTs} pr={row.prDefTs ?? 0.5} format="ppp" />
      ),
    },
    {
      key: 'defOreb',
      header: 'OREB%',
      tip: 'Opponent offensive rebound rate',
      render: (row) => row.isTotal ? (
        <td key="defOreb" className="total-cell">{row.defOreb.toFixed(1)}</td>
      ) : (
        <HeatCell key="defOreb" value={row.defOreb} pr={row.prDefOreb ?? 0.5} format="ppp" />
      ),
    },
    {
      key: 'defTov',
      header: 'TOV%',
      tip: 'Opponent turnover rate',
      render: (row) => row.isTotal ? (
        <td key="defTov" className="total-cell">{row.defTov.toFixed(1)}</td>
      ) : (
        <HeatCell key="defTov" value={row.defTov} pr={row.prDefTov ?? 0.5} format="ppp" />
      ),
    },
    {
      key: 'defFtr',
      header: 'FTR',
      tip: 'Opponent free throw rate',
      render: (row) => row.isTotal ? (
        <td key="defFtr" className="total-cell">{row.defFtr.toFixed(1)}</td>
      ) : (
        <HeatCell key="defFtr" value={row.defFtr} pr={row.prDefFtr ?? 0.5} format="ppp" />
      ),
    },
    {
      key: 'defPoss',
      header: 'Poss',
      tip: 'Defensive possessions',
      render: (row) => (
        <td key="defPoss" className={row.isTotal ? 'total-cell' : ''} style={{ color: 'var(--text-secondary)' }}>
          {row.defPoss.toLocaleString()}
        </td>
      ),
    },
    {
      key: 'minutes',
      header: 'Min',
      tip: 'Minutes played',
      sectionStart: true,
      render: (row) => (
        <td key="min" className={`section-start ${row.isTotal ? 'total-cell' : ''}`} style={{ color: 'var(--text-muted)' }}>
          {row.minutes.toFixed(1)}
        </td>
      ),
    },
    {
      key: 'totalPoss',
      header: 'Poss',
      tip: 'Total possessions',
      render: (row) => (
        <td key="totPoss" className={row.isTotal ? 'total-cell' : ''} style={{ color: 'var(--text-secondary)' }}>
          {row.totalPoss.toLocaleString()}
        </td>
      ),
    },
    {
      key: 'netRtg',
      header: 'Net',
      tip: 'Net rating',
      render: (row) => row.isTotal ? (
        <td key="net" className="total-cell">{fmtNet(row.netRtg)}</td>
      ) : (
        <HeatCell key="net" value={row.netRtg} pr={row.prNet ?? 0.5} format="net" bold />
      ),
    },
  ];
}

// ─── Formatting helpers ─────────────────────────────────────

function fmtNet(v: number): React.ReactNode {
  const s = v.toFixed(1);
  if (v > 0) return <span className="cell-pos">+{s}</span>;
  if (v < 0) return <span className="cell-neg">{s}</span>;
  return s;
}

function fmtPM(v: number): React.ReactNode {
  if (v > 0) return <span className="cell-pos">+{v}</span>;
  if (v < 0) return <span className="cell-neg">{v}</span>;
  return <span>{v}</span>;
}

function toTitleCase(s: string) {
  return s
    .toLowerCase()
    .replace(/\b([a-z])/g, (_, c: string) => c.toUpperCase());
}

function formatLineupNames(raw: string) {
  const parts = raw
    .split(',')
    .map(p => p.trim())
    .filter(Boolean);
  if (!parts.length) return raw;
  return parts.map(toTitleCase).join(' / ');
}

function formatTeamName(raw: string) {
  if (!raw) return '';
  return toTitleCase(raw.trim());
}
