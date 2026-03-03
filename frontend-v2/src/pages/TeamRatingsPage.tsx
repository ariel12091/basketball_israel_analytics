import { useState, useMemo, useCallback } from 'react';
import { useFilters, buildApiParams } from '../features/filters/store';
import { useApi } from '../hooks/useApi';
import { useSorting } from '../hooks/useSorting';
import DataTable, { exportCSV } from '../features/tables/DataTable';
import type { ColumnGroup, Column } from '../features/tables/DataTable';
import HeatCell from '../features/tables/HeatCell';
import type { TeamRating, TeamFourFactors } from '../types';

type ViewMode = 'summary' | 'ff';

export default function TeamRatingsPage() {
  const { state: filters } = useFilters();

  const [mode, setMode] = useState<ViewMode>('summary');
  const [explainerOpen, setExplainerOpen] = useState(false);

  // Clutch state (local, same pattern as LineupsPage)
  const [clutchEnabled, setClutchEnabled] = useState(false);
  const [clutchMargin, setClutchMargin] = useState(5);
  const [clutchStatus, setClutchStatus] = useState('all');
  const [clutchMinutes, setClutchMinutes] = useState(5);
  const [clutchOtMargin, setClutchOtMargin] = useState(false);

  // Build API params
  const apiParams = useMemo(() => {
    const base = buildApiParams(filters);
    const p: Record<string, unknown> = { ...base };
    if (clutchEnabled) {
      p.clutch_margin = clutchMargin;
      p.clutch_status = clutchStatus;
      p.clutch_minutes = clutchMinutes;
      if (clutchOtMargin) p.clutch_ot_margin = 'true';
    }
    return p;
  }, [filters, clutchEnabled, clutchMargin, clutchStatus, clutchMinutes, clutchOtMargin]);

  // Fetch data
  const { data: summaryData, loading: summaryLoading, error: summaryError } =
    useApi<TeamRating[]>('/api/teams/summary', apiParams, mode === 'summary');
  const { data: ffData, loading: ffLoading, error: ffError } =
    useApi<TeamFourFactors[]>('/api/teams/four-factors', apiParams, mode === 'ff');

  const summaryRows = summaryData ?? [];
  const ffRows = ffData ?? [];

  // Sorting
  const { sorted: summarySorted, sortKey: sSortKey, sortDir: sSortDir, onSort: sOnSort } =
    useSorting(summaryRows, 'netRtg', 'desc');
  const { sorted: ffSorted, sortKey: fSortKey, sortDir: fSortDir, onSort: fOnSort } =
    useSorting(ffRows, 'netRtg', 'desc');

  // Max rank for summary rank coloring
  const maxRank = useMemo(() => {
    if (!summaryRows.length) return 1;
    return Math.max(
      ...summaryRows.map(r => Math.max(r.rankNet || 0, r.rankOff || 0, r.rankDef || 0)),
      2,
    );
  }, [summaryRows]);

  // Summary columns
  const summaryColumns = useMemo(() => buildSummaryColumns(maxRank), [maxRank]);
  const ffColumns = useMemo(() => buildFFColumns(), []);

  // CSV export
  const handleSummaryExport = useCallback(() => {
    const keys = ['teamName', 'gamesPlayed', 'wins', 'losses', 'offPpp', 'defPpp', 'netRtg',
      'rankNet', 'rankOff', 'rankDef', 'offPace', 'defPace', 'offPoss', 'defPoss'];
    const headers = ['Team', 'GP', 'W', 'L', 'Off PPP', 'Def PPP', 'Net Rtg',
      'Net Rank', 'Off Rank', 'Def Rank', 'Off Pace', 'Def Pace', 'Off Poss', 'Def Poss'];
    exportCSV(summarySorted, 'team_ratings_summary.csv', keys, headers);
  }, [summarySorted]);

  const handleFFExport = useCallback(() => {
    const keys = ['teamName', 'offPpp', 'offTs', 'offOreb', 'offTov', 'offFtr', 'offPace', 'offPoss',
      'defPpp', 'defTs', 'defOreb', 'defTov', 'defFtr', 'defPace', 'defPoss', 'netRtg'];
    const headers = ['Team', 'Off PPP', 'Off TS%', 'Off OREB%', 'Off TOV%', 'Off FTR', 'Off Pace', 'Off Poss',
      'Def PPP', 'Def TS%', 'Def OREB%', 'Def TOV%', 'Def FTR', 'Def Pace', 'Def Poss', 'Net Rtg'];
    exportCSV(ffSorted, 'team_ratings_ff.csv', keys, headers);
  }, [ffSorted]);

  const loading = mode === 'summary' ? summaryLoading : ffLoading;
  const error = mode === 'summary' ? summaryError : ffError;
  const rows = mode === 'summary' ? summarySorted : ffSorted;

  return (
    <div className="panel-lineups">
      {/* Header */}
      <div className="section-header">
        <div className="section-title-area">
          <h2 className="section-title">Team Ratings</h2>
          <span className="section-subtitle">How do teams compare across the league?</span>
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
                <strong>Team Ratings</strong> show each team's offensive and defensive efficiency
                (points per 100 possessions) and net rating.
              </p>
              <p>
                <strong>Rank columns</strong> are colored green (best) to red (worst).
                <strong> Pace</strong> = possessions per 40 minutes of play.
              </p>
            </>
          ) : (
            <>
              <p style={{ marginBottom: 8 }}>
                <strong>Four Factors</strong> break down team performance: TS% (shooting efficiency),
                OREB% (offensive rebounding), TOV% (turnover rate), FTR (free throw rate).
              </p>
              <p>
                Background color indicates percentile rank among all teams. Green = better, red = worse.
              </p>
            </>
          )}
        </div>
      )}

      {/* Clutch controls */}
      <div className="lineup-controls">
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
                  <span>Margin &le; {clutchMargin}</span>
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
                  <span>&le; {clutchMinutes} min left</span>
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
        </div>
      </div>

      {/* Loading / Error / Empty */}
      {loading && (
        <div className="table-card">
          {[...Array(8)].map((_, i) => (
            <div className="skeleton-row" key={i}>
              <div className="skeleton-cell" style={{ width: 160 }} />
              <div className="skeleton-cell" style={{ width: 50 }} />
              <div className="skeleton-cell" style={{ width: 50 }} />
              <div className="skeleton-cell" style={{ width: 60 }} />
              <div className="skeleton-cell" style={{ width: 60 }} />
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
            No teams match the current filters.
          </p>
        </div>
      )}

      {/* Summary Table */}
      {mode === 'summary' && !loading && summarySorted.length > 0 && (
        <DataTable<TeamRating>
          groups={SUMMARY_GROUPS}
          columns={summaryColumns}
          data={summarySorted}
          sortKey={sSortKey}
          sortDir={sSortDir}
          onSort={sOnSort}
          infoText={`${summarySorted.length} teams`}
          onExport={handleSummaryExport}
          rowKey={(r) => String(r.teamId)}
        />
      )}

      {/* FF Table */}
      {mode === 'ff' && !loading && ffSorted.length > 0 && (
        <DataTable<TeamFourFactors>
          groups={FF_GROUPS}
          columns={ffColumns}
          data={ffSorted}
          sortKey={fSortKey}
          sortDir={fSortDir}
          onSort={fOnSort}
          infoText={`${ffSorted.length} teams`}
          onExport={handleFFExport}
          rowKey={(r) => String(r.teamId)}
        />
      )}
    </div>
  );
}

// ─── Rank color helper ────────────────────────────────────────
function rankColor(rank: number, maxRank: number): string {
  if (!rank || !maxRank || maxRank <= 1) return 'transparent';
  const t = (rank - 1) / (maxRank - 1); // 0=best, 1=worst
  // green → amber → red
  let r: number, g: number, b: number;
  if (t < 0.5) {
    const s = t * 2;
    r = Math.round(26 + s * (107 - 26));
    g = Math.round(107 + s * (90 - 107));
    b = Math.round(56 + s * (32 - 56));
  } else {
    const s = (t - 0.5) * 2;
    r = Math.round(107 + s * (139 - 107));
    g = Math.round(90 + s * (32 - 90));
    b = Math.round(32 + s * (32 - 32));
  }
  return `rgb(${r},${g},${b})`;
}

function toTitleCase(s: string) {
  return s.toLowerCase().replace(/\b([a-z])/g, (_, c: string) => c.toUpperCase());
}

// ─── Summary column definitions ──────────────────────────────

const SUMMARY_GROUPS: ColumnGroup[] = [
  { label: '', span: 1, empty: true },
  { label: 'RECORD', span: 3, sectionStart: true },
  { label: 'RATINGS', span: 6, sectionStart: true },
  { label: 'PACE', span: 2, sectionStart: true },
  { label: 'USAGE', span: 2, sectionStart: true },
];

function buildSummaryColumns(maxRank: number): Column<TeamRating>[] {
  return [
    {
      key: 'teamName',
      header: 'Team',
      sortable: true,
      render: (row) => (
        <td key="team" style={{ textAlign: 'left', fontFamily: 'var(--font-sans)', fontWeight: 600 }}>
          {toTitleCase(row.teamName)}
        </td>
      ),
    },
    {
      key: 'gamesPlayed',
      header: 'GP',
      tip: 'Games played',
      sectionStart: true,
      render: (row) => <td key="gp" className="section-start">{row.gamesPlayed}</td>,
    },
    {
      key: 'wins',
      header: 'W',
      render: (row) => <td key="w">{row.wins}</td>,
    },
    {
      key: 'losses',
      header: 'L',
      render: (row) => <td key="l">{row.losses}</td>,
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
      key: 'rankNet',
      header: 'Net Rank',
      tip: 'Net rating rank',
      render: (row) => (
        <td key="rankNet" style={{ background: rankColor(row.rankNet, maxRank), fontWeight: 600 }}>
          {row.rankNet}
        </td>
      ),
    },
    {
      key: 'rankOff',
      header: 'Off Rank',
      tip: 'Offensive PPP rank',
      render: (row) => (
        <td key="rankOff" style={{ background: rankColor(row.rankOff, maxRank), fontWeight: 600 }}>
          {row.rankOff}
        </td>
      ),
    },
    {
      key: 'rankDef',
      header: 'Def Rank',
      tip: 'Defensive PPP rank (1 = best defense)',
      render: (row) => (
        <td key="rankDef" style={{ background: rankColor(row.rankDef, maxRank), fontWeight: 600 }}>
          {row.rankDef}
        </td>
      ),
    },
    {
      key: 'offPace',
      header: 'Off Pace',
      tip: 'Offensive possessions per 40 minutes',
      sectionStart: true,
      render: (row) => (
        <td key="offPace" className="section-start" style={{ color: 'var(--text-secondary)' }}>
          {row.offPace ? row.offPace.toFixed(1) : '-'}
        </td>
      ),
    },
    {
      key: 'defPace',
      header: 'Def Pace',
      tip: 'Defensive possessions per 40 minutes',
      render: (row) => (
        <td key="defPace" style={{ color: 'var(--text-secondary)' }}>
          {row.defPace ? row.defPace.toFixed(1) : '-'}
        </td>
      ),
    },
    {
      key: 'offPoss',
      header: 'Off Poss',
      tip: 'Total offensive possessions',
      sectionStart: true,
      render: (row) => (
        <td key="offPoss" className="section-start" style={{ color: 'var(--text-muted)' }}>
          {row.offPoss.toLocaleString()}
        </td>
      ),
    },
    {
      key: 'defPoss',
      header: 'Def Poss',
      tip: 'Total defensive possessions',
      render: (row) => (
        <td key="defPoss" style={{ color: 'var(--text-muted)' }}>
          {row.defPoss.toLocaleString()}
        </td>
      ),
    },
  ];
}

// ─── FF column definitions ───────────────────────────────────

const FF_GROUPS: ColumnGroup[] = [
  { label: '', span: 1, empty: true },
  { label: 'OFFENSE', span: 7, sectionStart: true },
  { label: 'DEFENSE', span: 7, sectionStart: true },
  { label: 'NET', span: 1, sectionStart: true },
];

function buildFFColumns(): Column<TeamFourFactors>[] {
  return [
    {
      key: 'teamName',
      header: 'Team',
      sortable: true,
      render: (row) => (
        <td key="team" style={{ textAlign: 'left', fontFamily: 'var(--font-sans)', fontWeight: 600 }}>
          {toTitleCase(row.teamName)}
        </td>
      ),
    },
    // Offense
    {
      key: 'offPpp', header: 'PPP', tip: 'Offensive PPP', sectionStart: true,
      render: (row) => <HeatCell key="offPpp" value={row.offPpp} pr={row.prOffPpp} format="ppp" sectionStart />,
    },
    {
      key: 'offTs', header: 'TS%', tip: 'True Shooting %',
      render: (row) => <HeatCell key="offTs" value={row.offTs} pr={row.prOffTs} format="ppp" />,
    },
    {
      key: 'offOreb', header: 'OREB%', tip: 'Offensive rebound rate',
      render: (row) => <HeatCell key="offOreb" value={row.offOreb} pr={row.prOffOreb} format="ppp" />,
    },
    {
      key: 'offTov', header: 'TOV%', tip: 'Turnover rate',
      render: (row) => <HeatCell key="offTov" value={row.offTov} pr={row.prOffTov} format="ppp" />,
    },
    {
      key: 'offFtr', header: 'FTR', tip: 'Free throw rate',
      render: (row) => <HeatCell key="offFtr" value={row.offFtr} pr={row.prOffFtr} format="ppp" />,
    },
    {
      key: 'offPace', header: 'Pace', tip: 'Off possessions per 40 min',
      render: (row) => (
        <td key="offPace" style={{ color: 'var(--text-secondary)' }}>
          {row.offPace ? row.offPace.toFixed(1) : '-'}
        </td>
      ),
    },
    {
      key: 'offPoss', header: 'Poss', tip: 'Total offensive possessions',
      render: (row) => (
        <td key="offPoss" style={{ color: 'var(--text-muted)' }}>
          {row.offPoss.toLocaleString()}
        </td>
      ),
    },
    // Defense
    {
      key: 'defPpp', header: 'PPP', tip: 'Defensive PPP', sectionStart: true,
      render: (row) => <HeatCell key="defPpp" value={row.defPpp} pr={row.prDefPpp} format="ppp" sectionStart />,
    },
    {
      key: 'defTs', header: 'TS%', tip: 'Opponent TS%',
      render: (row) => <HeatCell key="defTs" value={row.defTs} pr={row.prDefTs} format="ppp" />,
    },
    {
      key: 'defOreb', header: 'OREB%', tip: 'Opponent OREB%',
      render: (row) => <HeatCell key="defOreb" value={row.defOreb} pr={row.prDefOreb} format="ppp" />,
    },
    {
      key: 'defTov', header: 'TOV%', tip: 'Opponent TOV%',
      render: (row) => <HeatCell key="defTov" value={row.defTov} pr={row.prDefTov} format="ppp" />,
    },
    {
      key: 'defFtr', header: 'FTR', tip: 'Opponent FTR',
      render: (row) => <HeatCell key="defFtr" value={row.defFtr} pr={row.prDefFtr} format="ppp" />,
    },
    {
      key: 'defPace', header: 'Pace', tip: 'Def possessions per 40 min',
      render: (row) => (
        <td key="defPace" style={{ color: 'var(--text-secondary)' }}>
          {row.defPace ? row.defPace.toFixed(1) : '-'}
        </td>
      ),
    },
    {
      key: 'defPoss', header: 'Poss', tip: 'Total defensive possessions',
      render: (row) => (
        <td key="defPoss" style={{ color: 'var(--text-muted)' }}>
          {row.defPoss.toLocaleString()}
        </td>
      ),
    },
    // Net
    {
      key: 'netRtg', header: 'Net', tip: 'Net rating', sectionStart: true,
      render: (row) => <HeatCell key="net" value={row.netRtg} pr={row.prNet} format="net" bold sectionStart />,
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
