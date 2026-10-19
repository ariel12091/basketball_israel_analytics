import { useContext, useEffect, useMemo, useState } from 'react';
import { apiClient } from '../api/client';
import { OnOffFourFactorsRow, OnOffSummaryRow } from '../api/contracts';
import { FilterContext } from '../features/filters/store';
import { PercentileTrack } from '../features/ui/PercentileTrack';
import { ShotSplitCell } from '../features/ui/ShotSplitCell';
import { ExplainerCard } from '../features/ui/ExplainerCard';
import { FourFactorsLegend } from '../features/ui/FourFactorsLegend';
import { ModeSwitch } from '../features/ui/ModeSwitch';
import { ShotLegend } from '../features/ui/ShotLegend';

type ViewMode = 'Summary' | 'Four Factors';

function asNumber(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim() !== '') {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return undefined;
}

function pickNumber(row: OnOffFourFactorsRow, keys: string[]): number | undefined {
  for (const key of keys) {
    const value = asNumber(row[key]);
    if (value !== undefined) return value;
  }
  return undefined;
}

function ffMetric(
  row: OnOffFourFactorsRow,
  config: { diff: string[]; onVal: string[]; offVal: string[]; onRank: string[]; offRank: string[]; diffRank: string[] },
) {
  return {
    diff: pickNumber(row, config.diff),
    onValue: pickNumber(row, config.onVal),
    offValue: pickNumber(row, config.offVal),
    onRank: pickNumber(row, config.onRank),
    offRank: pickNumber(row, config.offRank),
    diffRank: pickNumber(row, config.diffRank),
  };
}

export function OnOffPage() {
  const { state } = useContext(FilterContext);
  const filters = state.onoff;
  const [viewMode, setViewMode] = useState<ViewMode>('Summary');
  const [showExample, setShowExample] = useState(false);
  const [summaryRows, setSummaryRows] = useState<OnOffSummaryRow[]>([]);
  const [ffRows, setFfRows] = useState<OnOffFourFactorsRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const filterKey = useMemo(() => JSON.stringify(filters), [filters]);

  useEffect(() => {
    let cancelled = false;

    async function run() {
      setLoading(true);
      setError('');
      try {
        if (viewMode === 'Summary') {
          const response = await apiClient.onoffSummary({
            filters,
            mode: viewMode,
            extra: {
              teams: filters.teams,
              minAllPoss: filters.minAllPoss,
              minOnPoss: filters.minOnPoss,
            },
          });
          if (!cancelled) setSummaryRows(response.rows);
        } else {
          const response = await apiClient.onoffFourFactors({
            filters,
            mode: viewMode,
            extra: {
              teams: filters.teams,
              minAllPoss: filters.minAllPoss,
              minOnPoss: filters.minOnPoss,
            },
          });
          if (!cancelled) setFfRows(response.rows);
        }
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : 'Failed to load data.');
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    run();
    return () => {
      cancelled = true;
    };
  }, [filterKey, filters, viewMode]);

  const explainer = useMemo(() => {
    if (viewMode === 'Summary') {
      return {
        title: 'What This Tab Answers (Summary)',
        intro: 'Which players change team offense and defense most when they are on the floor?',
        bullets: [
          'Start broad, then add filters only if needed.',
          'Use Net/Off/Def impact first, then compare on-court vs off-court PPP.',
          'Validate that usage (ON/OFF possessions) is large enough.',
        ],
        example: (
          <>
            <p>
              Example: Hapoel Tel Aviv is better with Tyler Ennis on the floor by +29.5 per 100 possessions.
            </p>
            <p>
              That comes from offense +15.7 and defense -13.8 (lower allowed PPP is better on defense).
            </p>
            <p>
              On court: 129.4 scored, 93.2 allowed. Off court: 113.7 scored, 107.0 allowed.
            </p>
          </>
        ),
      };
    }

    return {
      title: 'What This Tab Answers (Four Factors)',
      intro: 'Why is player impact happening: shooting, rebounding, turnovers, or free-throw pressure?',
      bullets: [
        'Read offense and defense factors separately.',
        'Use TS%, OREB%, TOV%, and FTR together.',
        'Check possessions to avoid tiny-sample conclusions.',
      ],
      example: (
        <>
          <p>
            Example: Darius Hannah shows strong net impact with both offensive and defensive factor support.
          </p>
          <p>Offense rises on TS% and OREB%; defensive profile supports lower opponent efficiency.</p>
          <p>ON/OFF possession volume is substantial, so interpretation is more stable.</p>
        </>
      ),
    };
  }, [viewMode]);

  return (
    <section>
      <h2>On/Off Impact</h2>
      <ModeSwitch
        idPrefix="onoff-view"
        value={viewMode}
        onChange={(next) => {
          setViewMode(next);
          setShowExample(false);
        }}
      />

      <ExplainerCard
        title={explainer.title}
        intro={explainer.intro}
        bullets={explainer.bullets}
        open={showExample}
        onToggle={() => setShowExample((v) => !v)}
        example={explainer.example}
        exampleImageSrc={viewMode === 'Summary' ? '/snippets/onoff-row-snippet.png' : '/snippets/onoff-ff-row-snippet.png'}
        exampleImageAlt={viewMode === 'Summary' ? 'On/Off summary example row snippet' : 'On/Off four factors example row snippet'}
      />

      <section className="legends-stack" aria-label="On/Off legends">
        <h3 className="legends-title">Legends</h3>
        {viewMode === 'Summary' ? <ShotLegend /> : <FourFactorsLegend />}
      </section>

      <div className="table-placeholder">
        <h3>{viewMode} Table</h3>
        {loading ? <p className="status-line">Loading data...</p> : null}
        {!loading && error ? <p className="status-line status-error">{error}</p> : null}
        {!loading && !error && viewMode === 'Summary' && summaryRows.length === 0 ? (
          <p className="status-line">No rows matched these filters. Widen filters or lower possession thresholds.</p>
        ) : null}
        {!loading && !error && viewMode === 'Four Factors' && ffRows.length === 0 ? (
          <p className="status-line">No rows matched these filters. Widen filters or lower possession thresholds.</p>
        ) : null}

        {!loading && !error && viewMode === 'Summary' && summaryRows.length > 0 ? (
          <table className="simple-table">
            <thead>
              <tr>
                <th rowSpan={2}>Team</th>
                <th rowSpan={2}>Player</th>
                <th colSpan={3}>Net Impact</th>
                <th colSpan={5}>On Court Stats</th>
                <th colSpan={3}>Off Court Stats</th>
                <th colSpan={2}>Usage</th>
              </tr>
              <tr>
                <th>Net</th>
                <th>Off</th>
                <th>Def</th>
                <th>Off PPP</th>
                <th>Def PPP</th>
                <th>Net RTG</th>
                <th>Off Shot</th>
                <th>Def Shot</th>
                <th>Off PPP</th>
                <th>Def PPP</th>
                <th>Net RTG</th>
                <th>On Poss</th>
                <th>Off Poss</th>
              </tr>
            </thead>
            <tbody>
              {summaryRows.map((row) => (
                <tr key={row.id}>
                  <td>{row.team}</td>
                  <td>{row.player}</td>
                  <td>{row.net > 0 ? '+' : ''}{row.net.toFixed(1)}</td>
                  <td>{row.off > 0 ? '+' : ''}{row.off.toFixed(1)}</td>
                  <td>{row.def > 0 ? '+' : ''}{row.def.toFixed(1)}</td>
                  <td>{row.onOffPpp?.toFixed(1) ?? '-'}</td>
                  <td>{row.onDefPpp?.toFixed(1) ?? '-'}</td>
                  <td>{row.onNetRtg !== undefined ? `${row.onNetRtg > 0 ? '+' : ''}${row.onNetRtg.toFixed(1)}` : '-'}</td>
                  <td>{row.offOffPpp?.toFixed(1) ?? '-'}</td>
                  <td>{row.offDefPpp?.toFixed(1) ?? '-'}</td>
                  <td>{row.offNetRtg !== undefined ? `${row.offNetRtg > 0 ? '+' : ''}${row.offNetRtg.toFixed(1)}` : '-'}</td>
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
                  <td>{row.onPoss ?? '-'}</td>
                  <td>{row.offPoss ?? '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : null}

        {!loading && !error && viewMode === 'Four Factors' && ffRows.length > 0 ? (
          <table className="simple-table">
            <thead>
              <tr>
                <th rowSpan={2}>Team</th>
                <th rowSpan={2}>Player</th>
                <th colSpan={4}>Offense</th>
                <th colSpan={4}>Defense</th>
                <th colSpan={2}>Usage</th>
              </tr>
              <tr>
                <th>TS% Diff</th>
                <th>OREB% Diff</th>
                <th>TOV% Diff</th>
                <th>FTR Diff</th>
                <th>TS% Diff</th>
                <th>OREB% Diff</th>
                <th>TOV% Diff</th>
                <th>FTR Diff</th>
                <th>ON Poss</th>
                <th>OFF Poss</th>
              </tr>
            </thead>
            <tbody>
              {ffRows.map((row) => {
                const offTs = ffMetric(row, {
                  diff: ['offTsDiff', 'off_ts_diff', 'tsDiff'],
                  onVal: ['offOnTs', 'off_on_ts'],
                  offVal: ['offOffTs', 'off_off_ts'],
                  onRank: ['offOnTsRank', 'off_on_ts_rank', 'onPctRank'],
                  offRank: ['offOffTsRank', 'off_off_ts_rank', 'offPctRank'],
                  diffRank: ['prDiffOffTs', 'pr_diff_off_ts'],
                });
                const offOreb = ffMetric(row, {
                  diff: ['offOrebDiff', 'off_oreb_diff', 'orebDiff'],
                  onVal: ['offOnOreb', 'off_on_oreb'],
                  offVal: ['offOffOreb', 'off_off_oreb'],
                  onRank: ['offOnOrebRank', 'off_on_oreb_rank', 'onPctRank'],
                  offRank: ['offOffOrebRank', 'off_off_oreb_rank', 'offPctRank'],
                  diffRank: ['prDiffOffOreb', 'pr_diff_off_oreb'],
                });
                const offTov = ffMetric(row, {
                  diff: ['offTovDiff', 'off_tov_diff', 'tovDiff'],
                  onVal: ['offOnTov', 'off_on_tov'],
                  offVal: ['offOffTov', 'off_off_tov'],
                  onRank: ['offOnTovRank', 'off_on_tov_rank', 'onPctRank'],
                  offRank: ['offOffTovRank', 'off_off_tov_rank', 'offPctRank'],
                  diffRank: ['prDiffOffTov', 'pr_diff_off_tov'],
                });
                const offFtr = ffMetric(row, {
                  diff: ['offFtrDiff', 'off_ftr_diff', 'ftrDiff'],
                  onVal: ['offOnFtr', 'off_on_ftr'],
                  offVal: ['offOffFtr', 'off_off_ftr'],
                  onRank: ['offOnFtrRank', 'off_on_ftr_rank', 'onPctRank'],
                  offRank: ['offOffFtrRank', 'off_off_ftr_rank', 'offPctRank'],
                  diffRank: ['prDiffOffFtr', 'pr_diff_off_ftr'],
                });
                const defTs = ffMetric(row, {
                  diff: ['defTsDiff', 'def_ts_diff'],
                  onVal: ['defOnTs', 'def_on_ts'],
                  offVal: ['defOffTs', 'def_off_ts'],
                  onRank: ['defOnTsRank', 'def_on_ts_rank'],
                  offRank: ['defOffTsRank', 'def_off_ts_rank'],
                  diffRank: ['prDiffDefTs', 'pr_diff_def_ts'],
                });
                const defOreb = ffMetric(row, {
                  diff: ['defOrebDiff', 'def_oreb_diff'],
                  onVal: ['defOnOreb', 'def_on_oreb'],
                  offVal: ['defOffOreb', 'def_off_oreb'],
                  onRank: ['defOnOrebRank', 'def_on_oreb_rank'],
                  offRank: ['defOffOrebRank', 'def_off_oreb_rank'],
                  diffRank: ['prDiffDefOreb', 'pr_diff_def_oreb'],
                });
                const defTov = ffMetric(row, {
                  diff: ['defTovDiff', 'def_tov_diff'],
                  onVal: ['defOnTov', 'def_on_tov'],
                  offVal: ['defOffTov', 'def_off_tov'],
                  onRank: ['defOnTovRank', 'def_on_tov_rank'],
                  offRank: ['defOffTovRank', 'def_off_tov_rank'],
                  diffRank: ['prDiffDefTov', 'pr_diff_def_tov'],
                });
                const defFtr = ffMetric(row, {
                  diff: ['defFtrDiff', 'def_ftr_diff'],
                  onVal: ['defOnFtr', 'def_on_ftr'],
                  offVal: ['defOffFtr', 'def_off_ftr'],
                  onRank: ['defOnFtrRank', 'def_on_ftr_rank'],
                  offRank: ['defOffFtrRank', 'def_off_ftr_rank'],
                  diffRank: ['prDiffDefFtr', 'pr_diff_def_ftr'],
                });

                return (
                  <tr key={row.id}>
                    <td>{row.team}</td>
                    <td>{row.player}</td>
                    <td><PercentileTrack {...offTs} polarity="good-high" /></td>
                    <td><PercentileTrack {...offOreb} polarity="good-high" /></td>
                    <td><PercentileTrack {...offTov} polarity="good-low" /></td>
                    <td><PercentileTrack {...offFtr} polarity="good-high" /></td>
                    <td><PercentileTrack {...defTs} polarity="good-low" /></td>
                    <td><PercentileTrack {...defOreb} polarity="good-low" /></td>
                    <td><PercentileTrack {...defTov} polarity="good-high" /></td>
                    <td><PercentileTrack {...defFtr} polarity="good-low" /></td>
                    <td>{pickNumber(row, ['onPoss', 'off_on_poss'])?.toFixed(0) ?? '-'}</td>
                    <td>{pickNumber(row, ['offPoss', 'off_off_poss'])?.toFixed(0) ?? '-'}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        ) : null}
      </div>
    </section>
  );
}
