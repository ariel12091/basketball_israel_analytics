import { useContext, useEffect, useMemo, useState } from 'react';
import { apiClient } from '../api/client';
import { LineupFourFactorsRow, LineupGameRow, LineupSummaryRow } from '../api/contracts';
import { FilterContext } from '../features/filters/store';
import { ExplainerCard } from '../features/ui/ExplainerCard';
import { ModeSwitch } from '../features/ui/ModeSwitch';
import { FourFactorsLegend } from '../features/ui/FourFactorsLegend';
import { ShotSplitCell } from '../features/ui/ShotSplitCell';
import { ShotLegend } from '../features/ui/ShotLegend';

type ViewMode = 'Summary' | 'Four Factors';

export function LineupsPage() {
  const { state, setState } = useContext(FilterContext);
  const filters = state.lineups;
  const [viewMode, setViewMode] = useState<ViewMode>('Summary');
  const [showExample, setShowExample] = useState(false);
  const [selectedRow, setSelectedRow] = useState<LineupSummaryRow | LineupFourFactorsRow | null>(null);
  const [summaryRows, setSummaryRows] = useState<LineupSummaryRow[]>([]);
  const [ffRows, setFfRows] = useState<LineupFourFactorsRow[]>([]);
  const [tableLoading, setTableLoading] = useState(false);
  const [tableError, setTableError] = useState('');
  const [gamesRows, setGamesRows] = useState<LineupGameRow[]>([]);
  const [gamesLoading, setGamesLoading] = useState(false);
  const [gamesError, setGamesError] = useState('');
  const filterKey = useMemo(() => JSON.stringify(filters), [filters]);

  useEffect(() => {
    let cancelled = false;

    async function run() {
      setTableLoading(true);
      setTableError('');
      try {
        if (viewMode === 'Summary') {
          const response = await apiClient.lineupSummary({
            filters,
            mode: viewMode,
            extra: {
              playersOn: filters.playersOn,
              playersOff: filters.playersOff,
              groupSize: filters.groupSize,
              team: filters.team,
              minPoss: filters.minPoss,
              clutchEnabled: filters.clutchEnabled,
              clutchMargin: filters.clutchMargin,
              clutchStatus: filters.clutchStatus,
              clutchMinutes: filters.clutchMinutes,
              clutchOtMargin: filters.clutchOtMargin,
            },
          });
          if (!cancelled) setSummaryRows(response.rows);
        } else {
          const response = await apiClient.lineupFourFactors({
            filters,
            mode: viewMode,
            extra: {
              playersOn: filters.playersOn,
              playersOff: filters.playersOff,
              groupSize: filters.groupSize,
              team: filters.team,
              minPoss: filters.minPoss,
              clutchEnabled: filters.clutchEnabled,
              clutchMargin: filters.clutchMargin,
              clutchStatus: filters.clutchStatus,
              clutchMinutes: filters.clutchMinutes,
              clutchOtMargin: filters.clutchOtMargin,
            },
          });
          if (!cancelled) setFfRows(response.rows);
        }
      } catch (err) {
        if (!cancelled) setTableError(err instanceof Error ? err.message : 'Failed to load lineup data.');
      } finally {
        if (!cancelled) setTableLoading(false);
      }
    }

    run();
    return () => {
      cancelled = true;
    };
  }, [filterKey, filters, viewMode]);

  useEffect(() => {
    if (!selectedRow) return;
    const rowId = selectedRow.id;
    let cancelled = false;
    async function run() {
      setGamesLoading(true);
      setGamesError('');
      try {
        const response = await apiClient.lineupGames(rowId, {
          filters,
          mode: viewMode,
        });
        if (!cancelled) setGamesRows(response.rows);
      } catch (err) {
        if (!cancelled) setGamesError(err instanceof Error ? err.message : 'Failed to load lineup games.');
      } finally {
        if (!cancelled) setGamesLoading(false);
      }
    }
    run();
    return () => {
      cancelled = true;
    };
  }, [selectedRow, filters, viewMode]);

  const explainer = useMemo(() => {
    if (viewMode === 'Summary') {
      return {
        title: 'What This Tab Answers (Summary)',
        intro: 'Which player combinations perform best, and how reliable is the sample?',
        bullets: [
          'Pick a team first, then use players-on and players-off filters.',
          'Read Off PPP, Def PPP, and Net together.',
          'Prioritize lineups with meaningful minutes and possessions.',
        ],
        example: (
          <>
            <p>This lineup has Off PPP 119.3 and Def PPP 97.8, so Net RTG is +21.5.</p>
            <p>It outscored opponents by 21.5 points per 100 possessions while on court.</p>
            <p>Workload is meaningful: 93.7 minutes and 364 possessions.</p>
          </>
        ),
      };
    }

    return {
      title: 'What This Tab Answers (Four Factors)',
      intro: 'Which lineup-level factors drive good or bad results?',
      bullets: [
        'Use offense and defense factors together.',
        'Interpret TS%, OREB%, TOV%, and FTR as a profile, not isolation.',
        'Keep minutes and possessions in view before concluding.',
      ],
      example: (
        <>
          <p>The same lineup profile is strong across efficiency and possession-winning factors.</p>
          <p>Factor-level splits explain why Net impact remains positive.</p>
          <p>Sample size supports directional confidence.</p>
        </>
      ),
    };
  }, [viewMode]);

  return (
    <section>
      <h2>Lineup Data</h2>
      <ModeSwitch
        idPrefix="lineup-view"
        value={viewMode}
        onChange={(next) => {
          setViewMode(next);
          setShowExample(false);
        }}
      />

      <div className="group-size-inline">
        <span className="group-size-label">Group Size</span>
        <div className="chip-toggle-row">
          {(['2', '3', '4', '5'] as const).map((size) => (
            <button
              key={size}
              className={`toggle-chip ${filters.groupSize === size ? 'is-active' : ''}`}
              onClick={() =>
                setState((prev) => ({
                  ...prev,
                  lineups: { ...prev.lineups, groupSize: size },
                }))
              }
            >
              {size}
            </button>
          ))}
        </div>
      </div>

      <ExplainerCard
        title={explainer.title}
        intro={explainer.intro}
        bullets={explainer.bullets}
        open={showExample}
        onToggle={() => setShowExample((v) => !v)}
        example={explainer.example}
        exampleImageSrc={viewMode === 'Summary' ? '/snippets/lineup-row-snippet.png' : '/snippets/lineup-ff-row-snippet.png'}
        exampleImageAlt={viewMode === 'Summary' ? 'Lineup summary example row snippet' : 'Lineup four factors example row snippet'}
      />

      {viewMode === 'Summary' ? <ShotLegend /> : <FourFactorsLegend />}

      <div className="table-placeholder">
        <h3>Lineup Table (click row name to drill down)</h3>
        {tableLoading ? <p className="status-line">Loading lineup data...</p> : null}
        {!tableLoading && tableError ? <p className="status-line status-error">{tableError}</p> : null}
        {!tableLoading && !tableError && viewMode === 'Summary' && summaryRows.length === 0 ? (
          <p className="status-line">No rows matched current lineup filters.</p>
        ) : null}
        {!tableLoading && !tableError && viewMode === 'Four Factors' && ffRows.length === 0 ? (
          <p className="status-line">No rows matched current lineup filters.</p>
        ) : null}

        {!tableLoading && !tableError && viewMode === 'Summary' && summaryRows.length > 0 ? (
          <table className="simple-table">
            <thead>
              <tr>
                <th rowSpan={2}>Team</th>
                <th rowSpan={2}>Players</th>
                <th colSpan={4}>Summary</th>
                <th colSpan={4}>Offense</th>
                <th colSpan={4}>Defense</th>
              </tr>
              <tr>
                <th>Min</th>
                <th>Total Poss</th>
                <th>Net RTG</th>
                <th>+/-</th>
                <th>PPP</th>
                <th>Shot</th>
                <th>Poss</th>
                <th>Pts</th>
                <th>PPP</th>
                <th>Shot</th>
                <th>Poss</th>
                <th>Pts</th>
              </tr>
            </thead>
            <tbody>
              {summaryRows.map((row) => (
                <tr key={row.id}>
                  <td>{row.team}</td>
                  <td>
                    <button className="linklike" onClick={() => setSelectedRow(row)}>
                      {row.players}
                    </button>
                  </td>
                  <td>{row.min?.toFixed(1) ?? '-'}</td>
                  <td>{row.totalPoss ?? '-'}</td>
                  <td>{row.offPpp.toFixed(1)}</td>
                  <td>{row.defPpp.toFixed(1)}</td>
                  <td>{row.netRtg > 0 ? '+' : ''}{row.netRtg.toFixed(1)}</td>
                  <td>{row.plusMinus ?? '-'}</td>
                  <td>
                    {row.offFg2Pct !== undefined && row.offFg3Pct !== undefined && row.offFg2Acc !== undefined && row.offFg3Acc !== undefined ? (
                      <ShotSplitCell
                        fg2Pct={row.offFg2Pct}
                        fg3Pct={row.offFg3Pct}
                        fg2Acc={row.offFg2Acc}
                        fg3Acc={row.offFg3Acc}
                      />
                    ) : '-'}
                  </td>
                  <td>
                    {row.defFg2Pct !== undefined && row.defFg3Pct !== undefined && row.defFg2Acc !== undefined && row.defFg3Acc !== undefined ? (
                      <ShotSplitCell
                        fg2Pct={row.defFg2Pct}
                        fg3Pct={row.defFg3Pct}
                        fg2Acc={row.defFg2Acc}
                        fg3Acc={row.defFg3Acc}
                      />
                    ) : '-'}
                  </td>
                  <td>{row.offPoss ?? '-'}</td>
                  <td>{row.offPts ?? '-'}</td>
                  <td>{row.defPoss ?? '-'}</td>
                  <td>{row.defPts ?? '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : null}

        {!tableLoading && !tableError && viewMode === 'Four Factors' && ffRows.length > 0 ? (
          <table className="simple-table">
            <thead>
              <tr>
                <th rowSpan={2}>Team</th>
                <th rowSpan={2}>Players</th>
                <th colSpan={4}>Offense</th>
                <th colSpan={4}>Defense</th>
                <th rowSpan={2}>Net RTG</th>
              </tr>
              <tr>
                <th>TS%</th>
                <th>OREB%</th>
                <th>TOV%</th>
                <th>FTR</th>
                <th>TS%</th>
                <th>OREB%</th>
                <th>TOV%</th>
                <th>FTR</th>
              </tr>
            </thead>
            <tbody>
              {ffRows.map((row) => (
                <tr key={row.id}>
                  <td>{row.team}</td>
                  <td>
                    <button className="linklike" onClick={() => setSelectedRow(row)}>
                      {row.players}
                    </button>
                  </td>
                  <td>{row.tsOff.toFixed(1)}</td>
                  <td>{row.orebOff?.toFixed(1) ?? '-'}</td>
                  <td>{row.tovOff?.toFixed(1) ?? '-'}</td>
                  <td>{row.ftrOff?.toFixed(1) ?? '-'}</td>
                  <td>{row.tsDef.toFixed(1)}</td>
                  <td>{row.orebDef?.toFixed(1) ?? '-'}</td>
                  <td>{row.tovDef?.toFixed(1) ?? '-'}</td>
                  <td>{row.ftrDef?.toFixed(1) ?? '-'}</td>
                  <td>{row.netRtg > 0 ? '+' : ''}{row.netRtg.toFixed(1)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : null}

        {selectedRow ? (
          <div className="example-box">
            <h4>Lineup Drilldown</h4>
            <p>
              Selected lineup: <strong>{selectedRow.players}</strong>
            </p>
            <p>
              Current mode: <strong>{viewMode}</strong>
            </p>
            {gamesLoading ? <p className="status-line">Loading lineup games...</p> : null}
            {!gamesLoading && gamesError ? <p className="status-line status-error">{gamesError}</p> : null}
            {!gamesLoading && !gamesError && gamesRows.length === 0 ? (
              <p className="status-line">No game data found for this lineup.</p>
            ) : null}
            {!gamesLoading && !gamesError && gamesRows.length > 0 ? (
              <table className="simple-table">
                <thead>
                  <tr>
                    <th>GN</th>
                    <th>Date</th>
                    <th>Opponent</th>
                    <th>Score</th>
                    <th>W/L</th>
                  </tr>
                </thead>
                <tbody>
                  {gamesRows.map((row) => (
                    <tr key={row.id}>
                      <td>{row.gn}</td>
                      <td>{row.date}</td>
                      <td>{row.opponent}</td>
                      <td>{row.score}</td>
                      <td>{row.result}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : null}
            <button onClick={() => setSelectedRow(null)}>Close</button>
          </div>
        ) : null}
      </div>
    </section>
  );
}
