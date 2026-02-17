import { useState, useMemo, useCallback, useEffect, useRef } from 'react';
import { useFilters, buildApiParams } from '../features/filters/store';
import { useApi } from '../hooks/useApi';
import { useSorting } from '../hooks/useSorting';
import DataTable, { exportCSV } from '../features/tables/DataTable';
import type { ColumnGroup, Column } from '../features/tables/DataTable';
import HeatCell from '../features/tables/HeatCell';
import ShotCell from '../features/tables/ShotCell';
import FFCell from '../features/tables/FFCell';
import type { OnOffPlayer, OnOffFourFactors } from '../types';
import { autoMinPoss, adaptiveBaseline, percentileRank, computeShotAvgs } from '../utils/ranking';

type ViewMode = 'summary' | 'ff';

export default function OnOffPage() {
  const [mode, setMode] = useState<ViewMode>('summary');
  const [explainerOpen, setExplainerOpen] = useState(false);
  const { state: filters, dispatch } = useFilters();
  const autoEnabled = useRef(true);
  const autoUpdating = useRef(false); // guard: true while auto is dispatching
  const prevMinOnPoss = useRef(filters.minOnPoss);

  const apiParams = useMemo(() => buildApiParams(filters), [filters]);

  const { data: summaryRaw, loading: summaryLoading, error: summaryError } = useApi<OnOffPlayer[]>(
    '/api/onoff/summary',
    apiParams,
    mode === 'summary'
  );

  const { data: ffRaw, loading: ffLoading, error: ffError } = useApi<OnOffFourFactors[]>(
    '/api/onoff/four-factors',
    apiParams,
    mode === 'ff'
  );

  // Detect manual slider changes: if minOnPoss changed and auto didn't cause it → disable auto
  useEffect(() => {
    if (filters.minOnPoss !== prevMinOnPoss.current) {
      if (!autoUpdating.current) {
        autoEnabled.current = false;
      }
      autoUpdating.current = false;
      prevMinOnPoss.current = filters.minOnPoss;
    }
  }, [filters.minOnPoss]);

  // Auto-threshold: adjust minOnPoss so at least top 35% by usage is shown
  useEffect(() => {
    if (!autoEnabled.current) return;
    const data = mode === 'summary'
      ? (summaryRaw ?? []).map(p => ({ poss: p.onPoss }))
      : (ffRaw ?? []).map(p => ({ poss: p.offOnPoss }));
    if (!data.length) return;
    const needed = autoMinPoss(data);
    if (filters.minOnPoss > needed) {
      autoUpdating.current = true;
      dispatch({ type: 'SET_FIELD', field: 'minOnPoss', value: needed });
    }
  }, [summaryRaw, ffRaw, mode, dispatch, filters.minOnPoss]);

  // Re-enable auto when API-triggering filters change
  useEffect(() => {
    autoEnabled.current = true;
  }, [apiParams]);

  // Summary: ranks are pre-computed server-side (MV or onoff_compute).
  // Apply team + min poss filtering AFTER — so ranks stay stable when filtering.
  const summaryFiltered = useMemo(() => {
    if (!summaryRaw) return [];
    let data = summaryRaw;
    if (filters.teamIds.length > 0) {
      data = data.filter(p => filters.teamIds.includes(p.teamId));
    }
    return data.filter(
      p =>
        p.onPoss >= filters.minOnPoss &&
        p.onPoss >= filters.minAllPoss &&
        p.offPoss >= filters.minAllPoss,
    );
  }, [summaryRaw, filters.minOnPoss, filters.minAllPoss, filters.teamIds]);

  // (FF filtering happens after rank computation — see ffWithRanks below)

  // Compute percentile ranks on FULL dataset (before team/poss filtering)
  // This matches the Shiny app's ff_ranked_df() which ranks before local filters
  const ffRanked = useMemo(() => {
    if (!ffRaw?.length) return [];
    const possVec = ffRaw.map(p => p.offOnPoss);
    const thresh = adaptiveBaseline(possVec);

    // For each metric, build a values array where unqualified players get null
    const qualify = (vals: number[]) =>
      vals.map((v, i) => possVec[i] >= thresh ? v : null);

    // Dot position ranks: percentile rank of raw ON/OFF values (0-100 scale)
    const prOffTsOn = percentileRank(qualify(ffRaw.map(p => p.offOnTs)));
    const prOffTsOff = percentileRank(qualify(ffRaw.map(p => p.offOffTs)));
    const prOffOrebOn = percentileRank(qualify(ffRaw.map(p => p.offOnOreb)));
    const prOffOrebOff = percentileRank(qualify(ffRaw.map(p => p.offOffOreb)));
    const prOffTovOn = percentileRank(qualify(ffRaw.map(p => p.offOnTov)));
    const prOffTovOff = percentileRank(qualify(ffRaw.map(p => p.offOffTov)));
    const prOffFtrOn = percentileRank(qualify(ffRaw.map(p => p.offOnFtr)));
    const prOffFtrOff = percentileRank(qualify(ffRaw.map(p => p.offOffFtr)));
    const prDefTsOn = percentileRank(qualify(ffRaw.map(p => p.defOnTs)));
    const prDefTsOff = percentileRank(qualify(ffRaw.map(p => p.defOffTs)));
    const prDefOrebOn = percentileRank(qualify(ffRaw.map(p => p.defOnOreb)));
    const prDefOrebOff = percentileRank(qualify(ffRaw.map(p => p.defOffOreb)));
    const prDefTovOn = percentileRank(qualify(ffRaw.map(p => p.defOnTov)));
    const prDefTovOff = percentileRank(qualify(ffRaw.map(p => p.defOffTov)));
    const prDefFtrOn = percentileRank(qualify(ffRaw.map(p => p.defOnFtr)));
    const prDefFtrOff = percentileRank(qualify(ffRaw.map(p => p.defOffFtr)));

    // PPP diff ranks (for the 3 HeatCell diff columns)
    const prNetDiff = percentileRank(qualify(ffRaw.map(p => p.netRtgDiff)));
    const prOffDiff = percentileRank(qualify(ffRaw.map(p => p.offDiff)));
    const prDefDiff = percentileRank(qualify(ffRaw.map(p => p.defDiff)));

    // Metric diff ranks (for background heat on each FF column)
    // Shiny uses COLS_GRAD for "higher=green" and COLS_REV for "higher=red"
    const prDiffOffTs = percentileRank(qualify(ffRaw.map(p => p.offTsDiff)));
    const prDiffOffOreb = percentileRank(qualify(ffRaw.map(p => p.offOrebDiff)));
    const prDiffOffTov = percentileRank(qualify(ffRaw.map(p => p.offTovDiff)));
    const prDiffOffFtr = percentileRank(qualify(ffRaw.map(p => p.offFtrDiff)));
    const prDiffDefTs = percentileRank(qualify(ffRaw.map(p => p.defTsDiff)));
    const prDiffDefOreb = percentileRank(qualify(ffRaw.map(p => p.defOrebDiff)));
    const prDiffDefTov = percentileRank(qualify(ffRaw.map(p => p.defTovDiff)));
    const prDiffDefFtr = percentileRank(qualify(ffRaw.map(p => p.defFtrDiff)));

    // Helper: null-safe 0-100 scale, optionally inverted
    const s = (pr: number | null, inv = false) =>
      pr === null ? null : (inv ? (1 - pr) : pr) * 100;
    // Helper: null-safe 0-1 scale for heat backgrounds, optionally inverted (COLS_REV)
    const h = (pr: number | null, inv = false) =>
      pr === null ? null : inv ? (1 - pr) : pr;

    return ffRaw.map((p, i) => ({
      ...p,
      // Dot positions (0-100 scale, null = unranked)
      _offTsOnR: s(prOffTsOn[i]),
      _offTsOffR: s(prOffTsOff[i]),
      _offOrebOnR: s(prOffOrebOn[i]),
      _offOrebOffR: s(prOffOrebOff[i]),
      _offTovOnR: s(prOffTovOn[i], true),
      _offTovOffR: s(prOffTovOff[i], true),
      _offFtrOnR: s(prOffFtrOn[i]),
      _offFtrOffR: s(prOffFtrOff[i]),
      _defTsOnR: s(prDefTsOn[i], true),
      _defTsOffR: s(prDefTsOff[i], true),
      _defOrebOnR: s(prDefOrebOn[i], true),
      _defOrebOffR: s(prDefOrebOff[i], true),
      _defTovOnR: s(prDefTovOn[i]),
      _defTovOffR: s(prDefTovOff[i]),
      _defFtrOnR: s(prDefFtrOn[i], true),
      _defFtrOffR: s(prDefFtrOff[i], true),
      // PPP diff background heat (0-1 scale)
      _prNetDiff: prNetDiff[i],
      _prOffDiff: prOffDiff[i],
      _prDefDiff: prDefDiff[i],
      // Metric diff background heat (0-1 scale, with polarity)
      _hOffTs: h(prDiffOffTs[i]),           // higher = green
      _hOffOreb: h(prDiffOffOreb[i]),       // higher = green
      _hOffTov: h(prDiffOffTov[i], true),   // higher = red (COLS_REV)
      _hOffFtr: h(prDiffOffFtr[i]),         // higher = green
      _hDefTs: h(prDiffDefTs[i], true),     // higher = red (COLS_REV)
      _hDefOreb: h(prDiffDefOreb[i], true), // higher = red (COLS_REV)
      _hDefTov: h(prDiffDefTov[i]),         // higher = green (opponent TOV = good)
      _hDefFtr: h(prDiffDefFtr[i], true),   // higher = red (COLS_REV)
    }));
  }, [ffRaw]);

  // Apply team + min poss filtering AFTER ranks are computed (stable rankings)
  const ffWithRanks = useMemo(() => {
    let data = ffRanked;
    if (filters.teamIds.length > 0) {
      data = data.filter(p => filters.teamIds.includes(p.teamId));
    }
    return data.filter(
      p =>
        p.offOnPoss >= filters.minOnPoss &&
        p.offOnPoss >= filters.minAllPoss &&
        p.offOffPoss >= filters.minAllPoss,
    );
  }, [ffRanked, filters.minOnPoss, filters.minAllPoss, filters.teamIds]);

  // Compute dynamic shot averages from full dataset (qualifying players >= 50 FGA)
  const summaryColumns = useMemo(() => {
    const raw = summaryRaw ?? [];
    return buildSummaryColumns(
      computeShotAvgs(raw, 'offOnFg2Made', 'offOnFg2Att', 'offOnFg3Made', 'offOnFg3Att'),
      computeShotAvgs(raw, 'defOnFg2Made', 'defOnFg2Att', 'defOnFg3Made', 'defOnFg3Att'),
      computeShotAvgs(raw, 'offOffFg2Made', 'offOffFg2Att', 'offOffFg3Made', 'offOffFg3Att'),
      computeShotAvgs(raw, 'defOffFg2Made', 'defOffFg2Att', 'defOffFg3Made', 'defOffFg3Att'),
    );
  }, [summaryRaw]);

  // Sorting
  const { sorted: summarySorted, sortKey: sSortKey, sortDir: sSortDir, onSort: sOnSort } =
    useSorting(summaryFiltered, 'netDiff', 'desc');

  const { sorted: ffSorted, sortKey: fSortKey, sortDir: fSortDir, onSort: fOnSort } =
    useSorting(ffWithRanks, 'netRtgDiff', 'desc');

  // CSV export — only visible columns, no internal _pr/_h fields
  const handleSummaryExport = useCallback(() => {
    const keys = ['team', 'firstName', 'lastName', 'netDiff', 'offOnDiff', 'defOnDiff',
      'offOnPpp', 'defOnPpp', 'onNetRtg',
      'offOnFg2Made', 'offOnFg2Att', 'offOnFg3Made', 'offOnFg3Att',
      'defOnFg2Made', 'defOnFg2Att', 'defOnFg3Made', 'defOnFg3Att',
      'offOffPpp', 'defOffPpp', 'offNetRtg',
      'offOffFg2Made', 'offOffFg2Att', 'offOffFg3Made', 'offOffFg3Att',
      'defOffFg2Made', 'defOffFg2Att', 'defOffFg3Made', 'defOffFg3Att',
      'onPoss', 'offPoss'];
    const headers = ['Team', 'First Name', 'Last Name', 'Net RTG Diff', 'Off ON Diff', 'Def ON Diff',
      'Off ON PPP', 'Def ON PPP', 'On Net RTG',
      'Off ON FG2 Made', 'Off ON FG2 Att', 'Off ON FG3 Made', 'Off ON FG3 Att',
      'Def ON FG2 Made', 'Def ON FG2 Att', 'Def ON FG3 Made', 'Def ON FG3 Att',
      'Off OFF PPP', 'Def OFF PPP', 'Off Net RTG',
      'Off OFF FG2 Made', 'Off OFF FG2 Att', 'Off OFF FG3 Made', 'Off OFF FG3 Att',
      'Def OFF FG2 Made', 'Def OFF FG2 Att', 'Def OFF FG3 Made', 'Def OFF FG3 Att',
      'ON Poss', 'OFF Poss'];
    exportCSV(summarySorted, 'onoff_summary.csv', keys, headers);
  }, [summarySorted]);

  const handleFFExport = useCallback(() => {
    const keys = ['teamName', 'firstName', 'lastName', 'netRtgDiff', 'offDiff',
      'offTsDiff', 'offOrebDiff', 'offTovDiff', 'offFtrDiff',
      'defDiff', 'defTsDiff', 'defOrebDiff', 'defTovDiff', 'defFtrDiff',
      'offOnPoss', 'offOffPoss'];
    const headers = ['Team', 'First Name', 'Last Name', 'Net Diff', 'Off Rtg Diff',
      'Off TS% Diff', 'Off OREB% Diff', 'Off TOV% Diff', 'Off FTR Diff',
      'Def Rtg Diff', 'Def TS% Diff', 'Def OREB% Diff', 'Def TOV% Diff', 'Def FTR Diff',
      'ON Poss', 'OFF Poss'];
    exportCSV(ffSorted, 'onoff_four_factors.csv', keys, headers);
  }, [ffSorted]);

  const loading = mode === 'summary' ? summaryLoading : ffLoading;
  const error = mode === 'summary' ? summaryError : ffError;

  return (
    <div>
      {/* Header */}
      <div className="section-header">
        <div className="section-title-area">
          <h2 className="section-title">On/Off Impact</h2>
          <span className="section-subtitle">Who changes the game when they step on court?</span>
        </div>
        <div className="mode-toggle">
          <button
            className={`mode-btn ${mode === 'summary' ? 'active' : ''}`}
            onClick={() => setMode('summary')}
          >
            Summary
          </button>
          <button
            className={`mode-btn ${mode === 'ff' ? 'active' : ''}`}
            onClick={() => setMode('ff')}
          >
            Four Factors
          </button>
        </div>
      </div>

      <div className="lineup-controls">
        <div className="lineup-filter-row">
          <div className="lineup-filter-item" style={{ minWidth: 220 }}>
            <label className="lineup-filter-label">
              Min ON Poss{' '}
              <span style={{ color: 'var(--accent)', fontFamily: 'var(--font-mono)' }}>
                {filters.minOnPoss}
              </span>
            </label>
            <input
              type="range"
              className="lineup-slider"
              min={0}
              max={3000}
              step={10}
              value={filters.minOnPoss}
              onChange={e =>
                dispatch({
                  type: 'SET_FIELD',
                  field: 'minOnPoss',
                  value: parseInt(e.target.value),
                })
              }
            />
          </div>
          <div className="lineup-filter-item" style={{ minWidth: 220 }}>
            <label className="lineup-filter-label">
              Min All Poss{' '}
              <span style={{ color: 'var(--accent)', fontFamily: 'var(--font-mono)' }}>
                {filters.minAllPoss}
              </span>
            </label>
            <input
              type="range"
              className="lineup-slider"
              min={0}
              max={2000}
              step={10}
              value={filters.minAllPoss}
              onChange={e =>
                dispatch({
                  type: 'SET_FIELD',
                  field: 'minAllPoss',
                  value: parseInt(e.target.value),
                })
              }
            />
          </div>
        </div>
      </div>

      {/* Explainer */}
      <div
        className={`explainer-bar ${explainerOpen ? 'open' : ''}`}
        onClick={() => setExplainerOpen(!explainerOpen)}
      >
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
                <strong>Net Impact</strong> = Offense improvement + Defense improvement when on court.
                A player with Net +15.0 means their team outscores opponents by 15 more points per
                100 possessions when that player is on the floor.
              </p>
              <p style={{ marginBottom: 8 }}>
                <strong>Shooting columns</strong> show 2PT/3PT frequency and accuracy. FG% color:{' '}
                <span style={{ color: 'var(--positive)' }}>green = above average</span>,{' '}
                <span style={{ color: 'var(--negative)' }}>red = below average</span>.
              </p>
              <p>
                <strong>Tip:</strong> Start with Net Impact to identify high-impact players, then drill
                into On Court / Off Court stats to understand <em>why</em>.
              </p>
            </>
          ) : (
            <>
              <p style={{ marginBottom: 8 }}>
                <strong>Four Factors</strong> measure efficiency through TS% (shooting), OREB%
                (offensive rebounding), TOV% (turnovers), and FTR (free throw rate).
              </p>
              <p style={{ marginBottom: 8 }}>
                Each column shows the <strong>on-off difference</strong> with a range track.
                White dot = on-court, hollow dot = off-court. A wider gap = more impact.
              </p>
              <p>
                <strong>Color:</strong> Offense metrics are{' '}
                <span style={{ color: 'var(--positive)' }}>green when high</span> (except TOV% which
                is <span style={{ color: 'var(--negative)' }}>red when high</span>). Defense metrics
                reverse this.
              </p>
            </>
          )}
        </div>
      )}

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
      {mode === 'ff' && (
        <div className="legend-row">
          <span style={{ fontWeight: 600, color: 'var(--text-secondary)' }}>Four Factors:</span>
          <div className="legend-item">
            <div
              className="legend-dot"
              style={{ background: 'var(--text-primary)', border: '1.5px solid var(--bg-card)', width: 8, height: 8 }}
            />
            <span>On-Court (white)</span>
          </div>
          <div className="legend-item">
            <div
              className="legend-dot"
              style={{ background: 'var(--bg-card)', border: '1.5px solid var(--text-muted)', width: 6, height: 6 }}
            />
            <span>Off-Court (hollow)</span>
          </div>
          <div className="legend-item">
            <div className="legend-track-sample">
              <div className="dot-off" />
              <div className="dot-on" />
            </div>
            <span>Range track (0-100% rank)</span>
          </div>
        </div>
      )}

      {/* Loading skeleton */}
      {loading && (
        <div className="table-card">
          {[...Array(8)].map((_, i) => (
            <div className="skeleton-row" key={i}>
              <div className="skeleton-cell" style={{ width: 80 }} />
              <div className="skeleton-cell" style={{ width: 120 }} />
              <div className="skeleton-cell" style={{ width: 60 }} />
              <div className="skeleton-cell" style={{ width: 60 }} />
              <div className="skeleton-cell" style={{ width: 60 }} />
              <div className="skeleton-cell" style={{ width: 90 }} />
              <div className="skeleton-cell" style={{ width: 60 }} />
            </div>
          ))}
        </div>
      )}

      {/* Error message */}
      {error && !loading && (
        <div className="table-card" style={{ padding: '32px 0', textAlign: 'center' }}>
          <p style={{ color: 'var(--negative)', fontWeight: 600 }}>Failed to load data</p>
          <p style={{ color: 'var(--text-muted)', fontSize: 12, marginTop: 4 }}>{error}</p>
        </div>
      )}

      {/* Empty data message */}
      {!loading && !error && (
        (mode === 'summary' && summarySorted.length === 0 && summaryRaw !== null) ||
        (mode === 'ff' && ffSorted.length === 0 && ffRaw !== null)
      ) && (
        <div className="table-card" style={{ padding: '32px 0', textAlign: 'center' }}>
          <p style={{ color: 'var(--text-muted)' }}>
            No players match the current filters. Try reducing possession minimums or widening the date range.
          </p>
        </div>
      )}

      {/* Summary Table */}
      {mode === 'summary' && !loading && summarySorted.length > 0 && (
        <DataTable<OnOffPlayer>
          groups={SUMMARY_GROUPS}
          columns={summaryColumns}
          data={summarySorted}
          sortKey={sSortKey}
          sortDir={sSortDir}
          onSort={sOnSort}
          infoText={`Showing ${summarySorted.length} players · sorted by ${sSortKey === 'netDiff' ? 'Net Impact' : sSortKey}`}
          onExport={handleSummaryExport}
          rowKey={(p, i) => `${p.teamId}-${p.playerId}-${p.firstName}-${p.lastName}-${i}`}
        />
      )}

      {/* Four Factors Table */}
      {mode === 'ff' && !loading && ffSorted.length > 0 && (
        <DataTable
          groups={FF_GROUPS}
          columns={ffColumns}
          data={ffSorted}
          sortKey={fSortKey}
          sortDir={fSortDir}
          onSort={fOnSort}
          infoText={`Showing ${ffSorted.length} players · sorted by ${fSortKey === 'netRtgDiff' ? 'Net Diff' : fSortKey}`}
          onExport={handleFFExport}
          rowKey={(p, i) => `${p.teamId}-${p.playerId}-${p.firstName}-${p.lastName}-${i}`}
        />
      )}
    </div>
  );
}

// ============================================================
// Column definitions
// ============================================================

const SUMMARY_GROUPS: ColumnGroup[] = [
  { label: '', span: 2, empty: true },
  { label: 'NET IMPACT', span: 3, sectionStart: true },
  { label: 'ON COURT', span: 5, sectionStart: true },
  { label: 'OFF COURT', span: 5, sectionStart: true },
  { label: 'USAGE', span: 2, sectionStart: true },
];

interface ShotAvgs { avg2: number; avg3: number }

function buildSummaryColumns(
  offOnAvg: ShotAvgs, defOnAvg: ShotAvgs,
  offOffAvg: ShotAvgs, defOffAvg: ShotAvgs,
): Column<OnOffPlayer>[] { return [
  {
    key: 'team',
    header: 'Team',
    sortable: true,
    render: (p) => (
      <td
        key="team"
        style={{
          fontSize: 11,
          color: 'var(--text-secondary)',
          fontFamily: 'var(--font-sans)',
          minWidth: 80,
        }}
      >
        {p.team}
      </td>
    ),
  },
  {
    key: 'lastName',
    header: 'Player',
    sortable: true,
    render: (p) => (
      <td key="player" style={{ minWidth: 130 }}>
        {p.firstName} {p.lastName}
      </td>
    ),
  },
  {
    key: 'netDiff',
    header: 'Net',
    tip: 'Overall on-court impact per 100 poss',
    sectionStart: true,
    render: (p) => (
      <HeatCell key="net" value={p.netDiff} pr={p.prNet ?? 0.5} bold sectionStart />
    ),
  },
  {
    key: 'offOnDiff',
    header: 'Off',
    tip: 'Offense points per 100 improvement',
    render: (p) => (
      <HeatCell key="off" value={p.offOnDiff} pr={p.prOffOnD ?? 0.5} />
    ),
  },
  {
    key: 'defOnDiff',
    header: 'Def',
    tip: 'Defense points per 100 improvement (negative = good)',
    render: (p) => (
      <HeatCell key="def" value={p.defOnDiff} pr={1 - (p.prDefOnD ?? 0.5)} invert />
    ),
  },
  {
    key: 'offOnPpp',
    header: 'Off PPP',
    tip: 'Offensive PPP when player on court',
    sectionStart: true,
    render: (p) => (
      <HeatCell key="offOnPpp" value={p.offOnPpp} pr={p.prOffOn ?? 0.5} format="ppp" sectionStart />
    ),
  },
  {
    key: 'defOnPpp',
    header: 'Def PPP',
    tip: 'Defensive PPP when player on court',
    render: (p) => (
      <HeatCell key="defOnPpp" value={p.defOnPpp} pr={p.prDefOnInv ?? 0.5} format="ppp" />
    ),
  },
  {
    key: 'onNetRtg',
    header: 'Net',
    tip: 'Net rating on court',
    render: (p) => (
      <HeatCell key="onNet" value={p.onNetRtg} pr={p.prOnNet ?? 0.5} format="net" />
    ),
  },
  {
    key: 'offOnShot',
    header: 'Off Shot',
    tip: '2PT/3PT shooting splits on court',
    sortable: false,
    render: (p) => (
      <ShotCell
        key="offOnShot"
        fg2Made={p.offOnFg2Made}
        fg2Att={p.offOnFg2Att}
        fg3Made={p.offOnFg3Made}
        fg3Att={p.offOnFg3Att}
        avg2={offOnAvg.avg2}
        avg3={offOnAvg.avg3}
      />
    ),
  },
  {
    key: 'defOnShot',
    header: 'Def Shot',
    tip: 'Defensive shot splits allowed on court',
    sortable: false,
    render: (p) => (
      <ShotCell
        key="defOnShot"
        fg2Made={p.defOnFg2Made}
        fg2Att={p.defOnFg2Att}
        fg3Made={p.defOnFg3Made}
        fg3Att={p.defOnFg3Att}
        avg2={defOnAvg.avg2}
        avg3={defOnAvg.avg3}
        isDefense
      />
    ),
  },
  {
    key: 'offOffPpp',
    header: 'Off PPP',
    tip: 'Offensive PPP when player off court',
    sectionStart: true,
    render: (p) => (
      <HeatCell key="offOffPpp" value={p.offOffPpp} pr={p.prOffOff ?? 0.5} format="ppp" sectionStart />
    ),
  },
  {
    key: 'defOffPpp',
    header: 'Def PPP',
    tip: 'Defensive PPP when player off court',
    render: (p) => (
      <HeatCell key="defOffPpp" value={p.defOffPpp} pr={p.prDefOffInv ?? 0.5} format="ppp" />
    ),
  },
  {
    key: 'offNetRtg',
    header: 'Net',
    tip: 'Net rating off court',
    render: (p) => (
      <HeatCell key="offNet" value={p.offNetRtg} pr={p.prOffNet ?? 0.5} format="net" />
    ),
  },
  {
    key: 'offOffShot',
    header: 'Off Shot',
    tip: '2PT/3PT shooting splits off court',
    sortable: false,
    render: (p) => (
      <ShotCell
        key="offOffShot"
        fg2Made={p.offOffFg2Made}
        fg2Att={p.offOffFg2Att}
        fg3Made={p.offOffFg3Made}
        fg3Att={p.offOffFg3Att}
        avg2={offOffAvg.avg2}
        avg3={offOffAvg.avg3}
      />
    ),
  },
  {
    key: 'defOffShot',
    header: 'Def Shot',
    tip: 'Defensive shot splits off court',
    sortable: false,
    render: (p) => (
      <ShotCell
        key="defOffShot"
        fg2Made={p.defOffFg2Made}
        fg2Att={p.defOffFg2Att}
        fg3Made={p.defOffFg3Made}
        fg3Att={p.defOffFg3Att}
        avg2={defOffAvg.avg2}
        avg3={defOffAvg.avg3}
        isDefense
      />
    ),
  },
  {
    key: 'onPoss',
    header: 'On',
    tip: 'ON possessions',
    sectionStart: true,
    render: (p) => (
      <td key="onPoss" className="section-start" style={{ color: 'var(--text-secondary)' }}>
        {p.onPoss.toLocaleString()}
      </td>
    ),
  },
  {
    key: 'offPoss',
    header: 'Off',
    tip: 'OFF possessions',
    render: (p) => (
      <td key="offPoss" style={{ color: 'var(--text-muted)' }}>
        {p.offPoss.toLocaleString()}
      </td>
    ),
  },
]; }

// Four Factors column definitions
const FF_GROUPS: ColumnGroup[] = [
  { label: '', span: 2, empty: true },
  { label: 'TOTAL', span: 1 },
  { label: 'OFFENSE IMPACT (ON-OFF)', span: 5, sectionStart: true },
  { label: 'DEFENSE IMPACT (ON-OFF)', span: 5, sectionStart: true },
  { label: 'USAGE', span: 2, sectionStart: true },
];

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type FFRow = OnOffFourFactors & Record<string, any>;

const ffColumns: Column<FFRow>[] = [
  {
    key: 'teamName',
    header: 'Team',
    sortable: true,
    render: (p) => (
      <td
        key="team"
        style={{
          fontSize: 11,
          color: 'var(--text-secondary)',
          fontFamily: 'var(--font-sans)',
          minWidth: 80,
        }}
      >
        {p.teamName}
      </td>
    ),
  },
  {
    key: 'lastName',
    header: 'Player',
    sortable: true,
    render: (p) => (
      <td key="player" style={{ minWidth: 130 }}>
        {p.firstName} {p.lastName}
      </td>
    ),
  },
  {
    key: 'netRtgDiff',
    header: 'Diff',
    tip: 'Overall net on-off diff',
    render: (p) => (
      <HeatCell key="netDiff" value={p.netRtgDiff} pr={(p as Record<string, number>)._prNetDiff ?? 0.5} bold />
    ),
  },
  {
    key: 'offDiff',
    header: 'Diff',
    tip: 'Rating on-off diff',
    sectionStart: true,
    render: (p) => (
      <HeatCell key="offDiff" value={p.offDiff} pr={(p as Record<string, number>)._prOffDiff ?? 0.5} sectionStart />
    ),
  },
  {
    key: 'offTsDiff',
    header: 'TS%',
    tip: 'True shooting % on-off diff',
    render: (p) => {
      const r = p as Record<string, number | null>;
      return <FFCell key="offTs" diff={p.offTsDiff} onVal={p.offOnTs} offVal={p.offOffTs}
        onRank={r._offTsOnR} offRank={r._offTsOffR} heatPr={r._hOffTs} />;
    },
  },
  {
    key: 'offOrebDiff',
    header: 'OREB%',
    tip: 'Offensive rebound % on-off diff',
    render: (p) => {
      const r = p as Record<string, number | null>;
      return <FFCell key="offOreb" diff={p.offOrebDiff} onVal={p.offOnOreb} offVal={p.offOffOreb}
        onRank={r._offOrebOnR} offRank={r._offOrebOffR} heatPr={r._hOffOreb} />;
    },
  },
  {
    key: 'offTovDiff',
    header: 'TOV%',
    tip: 'Turnover % on-off diff',
    render: (p) => {
      const r = p as Record<string, number | null>;
      return <FFCell key="offTov" diff={p.offTovDiff} onVal={p.offOnTov} offVal={p.offOffTov}
        onRank={r._offTovOnR} offRank={r._offTovOffR} heatPr={r._hOffTov} invert />;
    },
  },
  {
    key: 'offFtrDiff',
    header: 'FTR',
    tip: 'Free throw rate on-off diff',
    render: (p) => {
      const r = p as Record<string, number | null>;
      return <FFCell key="offFtr" diff={p.offFtrDiff} onVal={p.offOnFtr} offVal={p.offOffFtr}
        onRank={r._offFtrOnR} offRank={r._offFtrOffR} heatPr={r._hOffFtr} />;
    },
  },
  {
    key: 'defDiff',
    header: 'Diff',
    tip: 'Rating on-off diff',
    sectionStart: true,
    render: (p) => (
      <HeatCell key="defDiff" value={p.defDiff} pr={(p as Record<string, number>)._prDefDiff ?? 0.5} sectionStart invert />
    ),
  },
  {
    key: 'defTsDiff',
    header: 'TS%',
    tip: 'Opponents TS% on-off diff',
    render: (p) => {
      const r = p as Record<string, number | null>;
      return <FFCell key="defTs" diff={p.defTsDiff} onVal={p.defOnTs} offVal={p.defOffTs}
        onRank={r._defTsOnR} offRank={r._defTsOffR} heatPr={r._hDefTs} invert />;
    },
  },
  {
    key: 'defOrebDiff',
    header: 'OREB%',
    tip: 'Opponents OREB% on-off diff',
    render: (p) => {
      const r = p as Record<string, number | null>;
      return <FFCell key="defOreb" diff={p.defOrebDiff} onVal={p.defOnOreb} offVal={p.defOffOreb}
        onRank={r._defOrebOnR} offRank={r._defOrebOffR} heatPr={r._hDefOreb} invert />;
    },
  },
  {
    key: 'defTovDiff',
    header: 'TOV%',
    tip: 'Opponents TOV% on-off diff',
    render: (p) => {
      const r = p as Record<string, number | null>;
      return <FFCell key="defTov" diff={p.defTovDiff} onVal={p.defOnTov} offVal={p.defOffTov}
        onRank={r._defTovOnR} offRank={r._defTovOffR} heatPr={r._hDefTov} />;
    },
  },
  {
    key: 'defFtrDiff',
    header: 'FTR',
    tip: 'Opponents FTR on-off diff',
    render: (p) => {
      const r = p as Record<string, number | null>;
      return <FFCell key="defFtr" diff={p.defFtrDiff} onVal={p.defOnFtr} offVal={p.defOffFtr}
        onRank={r._defFtrOnR} offRank={r._defFtrOffR} heatPr={r._hDefFtr} invert />;
    },
  },
  {
    key: 'offOnPoss',
    header: 'On',
    tip: 'ON possessions',
    sectionStart: true,
    render: (p) => (
      <td
        key="onPoss"
        className="section-start"
        style={{ color: 'var(--text-secondary)', fontFamily: 'var(--font-mono)' }}
      >
        {p.offOnPoss.toLocaleString()}
      </td>
    ),
  },
  {
    key: 'offOffPoss',
    header: 'Off',
    tip: 'OFF possessions',
    render: (p) => (
      <td key="offPoss" style={{ color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
        {p.offOffPoss.toLocaleString()}
      </td>
    ),
  },
];
