import { useState, useMemo, useCallback } from 'react';
import Select from 'react-select';
import type { StylesConfig } from 'react-select';
import { useFilters, buildApiParams } from '../features/filters/store';
import { useApi } from '../hooks/useApi';
import { useSorting } from '../hooks/useSorting';
import DataTable, { exportCSV } from '../features/tables/DataTable';
import type { ColumnGroup, Column } from '../features/tables/DataTable';
import ShotCell from '../features/tables/ShotCell';
import type { GameLogSummary, GameLogFourFactors, Team } from '../types';
import { computeShotAvgs } from '../utils/ranking';

type ViewMode = 'summary' | 'ff';

interface TeamOption {
  value: number;
  label: string;
}

const selectStyles: StylesConfig<TeamOption, false> = {
  control: (base, st) => ({
    ...base,
    background: 'var(--bg-elevated)',
    borderColor: st.isFocused ? 'var(--accent)' : 'var(--bg-hover)',
    boxShadow: st.isFocused ? '0 0 0 1px var(--accent)' : 'none',
    minHeight: 32,
    fontSize: 13,
    '&:hover': { borderColor: 'var(--accent)' },
  }),
  menu: (base) => ({
    ...base,
    background: 'var(--bg-elevated)',
    border: '1px solid var(--bg-hover)',
    zIndex: 20,
  }),
  option: (base, st) => ({
    ...base,
    background: st.isFocused ? 'var(--bg-hover)' : 'transparent',
    color: 'var(--text-primary)',
    fontSize: 13,
    cursor: 'pointer',
    '&:active': { background: 'var(--bg-active)' },
  }),
  singleValue: (base) => ({
    ...base,
    color: 'var(--text-primary)',
  }),
  input: (base) => ({
    ...base,
    color: 'var(--text-primary)',
  }),
  placeholder: (base) => ({
    ...base,
    color: 'var(--text-muted)',
    fontSize: 13,
  }),
  noOptionsMessage: (base) => ({
    ...base,
    color: 'var(--text-muted)',
  }),
};

export default function GameLogsPage() {
  const { state: filters } = useFilters();

  const [mode, setMode] = useState<ViewMode>('summary');
  const [explainerOpen, setExplainerOpen] = useState(false);

  // Local team dropdown (single-select, optional "All teams")
  const [filterTeamId, setFilterTeamId] = useState<number | null>(null);

  // Fetch teams for dropdown
  const metaParams = useMemo(() => ({ game_year: filters.gameYear }), [filters.gameYear]);
  const { data: teamsData } = useApi<Team[]>('/api/meta/teams', metaParams);
  const teamOptions: TeamOption[] = useMemo(
    () => (teamsData ?? []).map(t => ({ value: t.teamId, label: t.teamName })),
    [teamsData],
  );
  const selectedTeamOption = useMemo(
    () => teamOptions.find(o => o.value === filterTeamId) ?? null,
    [teamOptions, filterTeamId],
  );

  // Build API params
  const apiParams = useMemo(() => {
    const base = buildApiParams(filters);
    const p: Record<string, unknown> = { ...base };
    if (filterTeamId) p.filter_team_id = filterTeamId;
    return p;
  }, [filters, filterTeamId]);

  // Fetch data
  const { data: summaryData, loading: summaryLoading, error: summaryError } =
    useApi<GameLogSummary[]>('/api/gamelogs/summary', apiParams, mode === 'summary');
  const { data: ffData, loading: ffLoading, error: ffError } =
    useApi<GameLogFourFactors[]>('/api/gamelogs/four-factors', apiParams, mode === 'ff');

  const summaryRows = summaryData ?? [];
  const ffRows = ffData ?? [];

  // Shot averages for ShotCell (summary view)
  const offAvgs = useMemo(
    () => computeShotAvgs(summaryRows, 'offFg2Made', 'offFg2Att', 'offFg3Made', 'offFg3Att', 0),
    [summaryRows],
  );
  const defAvgs = useMemo(
    () => computeShotAvgs(summaryRows, 'defFg2Made', 'defFg2Att', 'defFg3Made', 'defFg3Att', 0),
    [summaryRows],
  );

  // Sorting
  const { sorted: summarySorted, sortKey: sSortKey, sortDir: sSortDir, onSort: sOnSort } =
    useSorting(summaryRows, 'gn', 'desc');
  const { sorted: ffSorted, sortKey: fSortKey, sortDir: fSortDir, onSort: fOnSort } =
    useSorting(ffRows, 'gn', 'desc');

  // Summary columns
  const summaryColumns = useMemo(
    () => buildSummaryColumns(offAvgs.avg2, offAvgs.avg3, defAvgs.avg2, defAvgs.avg3),
    [offAvgs, defAvgs],
  );
  const ffColumns = useMemo(() => buildFFColumns(), []);

  // CSV export
  const handleSummaryExport = useCallback(() => {
    const keys = ['gn', 'gameDate', 'teamName', 'opponent', 'result', 'score',
      'offPpp', 'defPpp', 'netRtg', 'offPoss', 'defPoss'];
    const headers = ['GN', 'Date', 'Team', 'Opponent', 'W/L', 'Score',
      'Off PPP', 'Def PPP', 'Net Rtg', 'Off Poss', 'Def Poss'];
    exportCSV(summarySorted, 'gamelogs_summary.csv', keys, headers);
  }, [summarySorted]);

  const handleFFExport = useCallback(() => {
    const keys = ['gn', 'gameDate', 'teamName', 'opponent', 'result', 'score',
      'offPpp', 'offTsPct', 'offOrebPct', 'offTovPct', 'offFtrPct',
      'defPpp', 'defTsPct', 'defOrebPct', 'defTovPct', 'defFtrPct',
      'offPoss', 'defPoss'];
    const headers = ['GN', 'Date', 'Team', 'Opponent', 'W/L', 'Score',
      'Off PPP', 'Off TS%', 'Off OREB%', 'Off TOV%', 'Off FTR',
      'Def PPP', 'Def TS%', 'Def OREB%', 'Def TOV%', 'Def FTR',
      'Off Poss', 'Def Poss'];
    exportCSV(ffSorted, 'gamelogs_ff.csv', keys, headers);
  }, [ffSorted]);

  const loading = mode === 'summary' ? summaryLoading : ffLoading;
  const error = mode === 'summary' ? summaryError : ffError;
  const rows = mode === 'summary' ? summarySorted : ffSorted;

  return (
    <div className="panel-lineups">
      {/* Header */}
      <div className="section-header">
        <div className="section-title-area">
          <h2 className="section-title">Game Logs</h2>
          <span className="section-subtitle">Per-game team performance breakdown</span>
        </div>
        <div className="mode-toggle">
          <button className={`mode-btn ${mode === 'summary' ? 'active' : ''}`} onClick={() => setMode('summary')}>Summary</button>
          <button className={`mode-btn ${mode === 'ff' ? 'active' : ''}`} onClick={() => setMode('ff')}>Four Factors</button>
        </div>
      </div>

      {/* Explainer */}
      <button
        type="button"
        className={`explainer-bar ${explainerOpen ? 'open' : ''}`}
        onClick={() => setExplainerOpen(!explainerOpen)}
      >
        <div className="explainer-bar-left">
          <div className="explainer-icon">?</div>
          <span className="explainer-bar-title">How to read this table</span>
        </div>
        <span className="explainer-chevron">&#9660;</span>
      </button>
      {explainerOpen && (
        <div className="explainer-body show">
          {mode === 'summary' ? (
            <>
              <p style={{ marginBottom: 8 }}>
                <strong>Game Logs</strong> show each team's performance in every game: offensive and defensive
                efficiency (PPP), net rating, and shooting splits.
              </p>
              <p>
                <strong>Shooting columns</strong> show 2PT/3PT frequency and accuracy. FG% color:{' '}
                <span style={{ color: 'var(--positive)' }}>green = above average</span>,{' '}
                <span style={{ color: 'var(--negative)' }}>red = below average</span>.
              </p>
            </>
          ) : (
            <>
              <p style={{ marginBottom: 8 }}>
                <strong>Four Factors</strong> break down each game: TS% (shooting efficiency),
                OREB% (offensive rebounding), TOV% (turnover rate), FTR (free throw rate).
              </p>
              <p>
                Values are per-game raw numbers (not ranked, since game-level data has high variance).
              </p>
            </>
          )}
        </div>
      )}

      {/* Controls: Team dropdown */}
      <div className="lineup-controls">
        <div className="lineup-filter-row">
          <div className="lineup-filter-item" style={{ minWidth: 180 }}>
            <label className="lineup-filter-label">Team</label>
            <Select<TeamOption, false>
              isClearable
              options={teamOptions}
              value={selectedTeamOption}
              onChange={(sel) => setFilterTeamId(sel ? sel.value : null)}
              placeholder="All teams"
              styles={selectStyles}
              classNamePrefix="rs"
            />
          </div>
        </div>
      </div>

      {/* Legend (summary only) */}
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
          {[...Array(10)].map((_, i) => (
            <div className="skeleton-row" key={i}>
              <div className="skeleton-cell" style={{ width: 40 }} />
              <div className="skeleton-cell" style={{ width: 80 }} />
              <div className="skeleton-cell" style={{ width: 120 }} />
              <div className="skeleton-cell" style={{ width: 120 }} />
              <div className="skeleton-cell" style={{ width: 40 }} />
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
      {!loading && !error && rows.length === 0 && (summaryData !== null || ffData !== null) && (
        <div className="table-card" style={{ padding: '32px 0', textAlign: 'center' }}>
          <p style={{ color: 'var(--text-muted)' }}>
            No games match the current filters.
          </p>
        </div>
      )}

      {/* Summary Table */}
      {mode === 'summary' && !loading && summarySorted.length > 0 && (
        <DataTable<GameLogSummary>
          groups={SUMMARY_GROUPS}
          columns={summaryColumns}
          data={summarySorted}
          sortKey={sSortKey}
          sortDir={sSortDir}
          onSort={sOnSort}
          infoText={`${summarySorted.length} games`}
          onExport={handleSummaryExport}
          rowKey={(r) => `${r.gameId}-${r.teamId}`}
        />
      )}

      {/* FF Table */}
      {mode === 'ff' && !loading && ffSorted.length > 0 && (
        <DataTable<GameLogFourFactors>
          groups={FF_GROUPS}
          columns={ffColumns}
          data={ffSorted}
          sortKey={fSortKey}
          sortDir={fSortDir}
          onSort={fOnSort}
          infoText={`${ffSorted.length} games`}
          onExport={handleFFExport}
          rowKey={(r) => `${r.gameId}-${r.teamId}`}
        />
      )}
    </div>
  );
}

// ─── Formatting helpers ─────────────────────────────────────

function fmtNet(v: number): React.ReactNode {
  const s = v.toFixed(1);
  if (v > 0) return <span className="cell-pos">+{s}</span>;
  if (v < 0) return <span className="cell-neg">{s}</span>;
  return s;
}

function toTitleCase(s: string) {
  return s.toLowerCase().replace(/\b([a-z])/g, (_, c: string) => c.toUpperCase());
}

// ─── Summary column definitions ──────────────────────────────

const SUMMARY_GROUPS: ColumnGroup[] = [
  { label: 'GAME INFO', span: 6 },
  { label: 'RATINGS', span: 3, sectionStart: true },
  { label: 'SHOTS', span: 2, sectionStart: true },
  { label: 'USAGE', span: 2, sectionStart: true },
];

function buildSummaryColumns(
  offAvg2: number, offAvg3: number,
  defAvg2: number, defAvg3: number,
): Column<GameLogSummary>[] {
  return [
    {
      key: 'gn',
      header: 'GN',
      tip: 'Game number',
      render: (row) => <td key="gn" style={{ fontWeight: 600 }}>{row.gn}</td>,
    },
    {
      key: 'gameDate',
      header: 'Date',
      render: (row) => (
        <td key="date" style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
          {row.gameDate}
        </td>
      ),
    },
    {
      key: 'teamName',
      header: 'Team',
      render: (row) => (
        <td key="team" style={{ textAlign: 'left', fontFamily: 'var(--font-sans)', fontWeight: 600 }}>
          {toTitleCase(row.teamName)}
        </td>
      ),
    },
    {
      key: 'opponent',
      header: 'Opponent',
      render: (row) => (
        <td key="opp" style={{ textAlign: 'left', fontFamily: 'var(--font-sans)' }}>
          {toTitleCase(row.opponent)}
        </td>
      ),
    },
    {
      key: 'result',
      header: 'W/L',
      render: (row) => (
        <td key="result">
          <span style={{
            fontWeight: 700,
            color: row.result === 'W' ? 'var(--positive)' : 'var(--negative)',
          }}>
            {row.result}
          </span>
        </td>
      ),
    },
    {
      key: 'score',
      header: 'Score',
      sortable: false,
      render: (row) => (
        <td key="score" style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
          {row.score}
        </td>
      ),
    },
    {
      key: 'offPpp',
      header: 'Off PPP',
      tip: 'Offensive points per 100 possessions',
      sectionStart: true,
      render: (row) => <td key="offPpp" className="section-start">{row.offPpp.toFixed(1)}</td>,
    },
    {
      key: 'defPpp',
      header: 'Def PPP',
      tip: 'Defensive points per 100 possessions',
      render: (row) => <td key="defPpp">{row.defPpp.toFixed(1)}</td>,
    },
    {
      key: 'netRtg',
      header: 'Net Rtg',
      tip: 'Net rating (Off - Def PPP)',
      render: (row) => (
        <td key="net" style={{ fontWeight: 700 }}>
          {fmtNet(row.netRtg)}
        </td>
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
          avg2={offAvg2} avg3={offAvg3}
          minFga={0}
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
          avg2={defAvg2} avg3={defAvg3}
          isDefense
          minFga={0}
        />
      ),
    },
    {
      key: 'offPoss',
      header: 'Off Poss',
      tip: 'Offensive possessions',
      sectionStart: true,
      render: (row) => (
        <td key="offPoss" className="section-start" style={{ color: 'var(--text-muted)' }}>
          {row.offPoss}
        </td>
      ),
    },
    {
      key: 'defPoss',
      header: 'Def Poss',
      tip: 'Defensive possessions',
      render: (row) => (
        <td key="defPoss" style={{ color: 'var(--text-muted)' }}>
          {row.defPoss}
        </td>
      ),
    },
  ];
}

// ─── FF column definitions ───────────────────────────────────

const FF_GROUPS: ColumnGroup[] = [
  { label: 'GAME INFO', span: 6 },
  { label: 'OFFENSE', span: 5, sectionStart: true },
  { label: 'DEFENSE', span: 5, sectionStart: true },
  { label: 'USAGE', span: 2, sectionStart: true },
];

function buildFFColumns(): Column<GameLogFourFactors>[] {
  return [
    {
      key: 'gn',
      header: 'GN',
      tip: 'Game number',
      render: (row) => <td key="gn" style={{ fontWeight: 600 }}>{row.gn}</td>,
    },
    {
      key: 'gameDate',
      header: 'Date',
      render: (row) => (
        <td key="date" style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
          {row.gameDate}
        </td>
      ),
    },
    {
      key: 'teamName',
      header: 'Team',
      render: (row) => (
        <td key="team" style={{ textAlign: 'left', fontFamily: 'var(--font-sans)', fontWeight: 600 }}>
          {toTitleCase(row.teamName)}
        </td>
      ),
    },
    {
      key: 'opponent',
      header: 'Opponent',
      render: (row) => (
        <td key="opp" style={{ textAlign: 'left', fontFamily: 'var(--font-sans)' }}>
          {toTitleCase(row.opponent)}
        </td>
      ),
    },
    {
      key: 'result',
      header: 'W/L',
      render: (row) => (
        <td key="result">
          <span style={{
            fontWeight: 700,
            color: row.result === 'W' ? 'var(--positive)' : 'var(--negative)',
          }}>
            {row.result}
          </span>
        </td>
      ),
    },
    {
      key: 'score',
      header: 'Score',
      sortable: false,
      render: (row) => (
        <td key="score" style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
          {row.score}
        </td>
      ),
    },
    // Offense
    {
      key: 'offPpp',
      header: 'PPP',
      tip: 'Offensive PPP',
      sectionStart: true,
      render: (row) => <td key="offPpp" className="section-start">{row.offPpp.toFixed(1)}</td>,
    },
    {
      key: 'offTsPct',
      header: 'TS%',
      tip: 'True Shooting %',
      render: (row) => <td key="offTs">{row.offTsPct.toFixed(1)}</td>,
    },
    {
      key: 'offOrebPct',
      header: 'OREB%',
      tip: 'Offensive rebound rate',
      render: (row) => <td key="offOreb">{row.offOrebPct.toFixed(1)}</td>,
    },
    {
      key: 'offTovPct',
      header: 'TOV%',
      tip: 'Turnover rate',
      render: (row) => <td key="offTov">{row.offTovPct.toFixed(1)}</td>,
    },
    {
      key: 'offFtrPct',
      header: 'FTR',
      tip: 'Free throw rate',
      render: (row) => <td key="offFtr">{row.offFtrPct.toFixed(1)}</td>,
    },
    // Defense
    {
      key: 'defPpp',
      header: 'PPP',
      tip: 'Defensive PPP',
      sectionStart: true,
      render: (row) => <td key="defPpp" className="section-start">{row.defPpp.toFixed(1)}</td>,
    },
    {
      key: 'defTsPct',
      header: 'TS%',
      tip: 'Opponent TS%',
      render: (row) => <td key="defTs">{row.defTsPct.toFixed(1)}</td>,
    },
    {
      key: 'defOrebPct',
      header: 'OREB%',
      tip: 'Opponent OREB%',
      render: (row) => <td key="defOreb">{row.defOrebPct.toFixed(1)}</td>,
    },
    {
      key: 'defTovPct',
      header: 'TOV%',
      tip: 'Opponent TOV%',
      render: (row) => <td key="defTov">{row.defTovPct.toFixed(1)}</td>,
    },
    {
      key: 'defFtrPct',
      header: 'FTR',
      tip: 'Opponent FTR',
      render: (row) => <td key="defFtr">{row.defFtrPct.toFixed(1)}</td>,
    },
    // Usage
    {
      key: 'offPoss',
      header: 'Off Poss',
      tip: 'Offensive possessions',
      sectionStart: true,
      render: (row) => (
        <td key="offPoss" className="section-start" style={{ color: 'var(--text-muted)' }}>
          {row.offPoss}
        </td>
      ),
    },
    {
      key: 'defPoss',
      header: 'Def Poss',
      tip: 'Defensive possessions',
      render: (row) => (
        <td key="defPoss" style={{ color: 'var(--text-muted)' }}>
          {row.defPoss}
        </td>
      ),
    },
  ];
}
