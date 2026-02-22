import { useEffect, useMemo } from 'react';
import type { LineupGameLog } from '../../types';
import ShotCell from './ShotCell';
import { computeShotAvgs } from '../../utils/ranking';
import { useApi } from '../../hooks/useApi';

interface LineupModalProps {
  subHash: string;
  teamId: number;
  gameYear: number;
  viewMode: 'summary' | 'ff';
  onClose: () => void;
}

export default function LineupModal({ subHash, teamId, gameYear, viewMode, onClose }: LineupModalProps) {
  const lineupParams = useMemo(
    () => ({
      sub_hash: subHash,
      team_id: teamId,
      game_year: gameYear,
      view_mode: viewMode,
    }),
    [subHash, teamId, gameYear, viewMode],
  );
  const { data, loading, error } = useApi<{ lineupName: string; games: LineupGameLog[] }>(
    '/api/lineups/game-log',
    lineupParams,
    true,
  );
  const lineupName = data?.lineupName || subHash;
  const games = data?.games || [];

  // Escape to close
  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  // Compute shot averages for summary modal
  const shotAvgs = viewMode === 'summary' ? {
    off: computeShotAvgs(games, 'offFg2Made' as keyof LineupGameLog, 'offFg2Att' as keyof LineupGameLog,
                          'offFg3Made' as keyof LineupGameLog, 'offFg3Att' as keyof LineupGameLog, 10),
    def: computeShotAvgs(games, 'defFg2Made' as keyof LineupGameLog, 'defFg2Att' as keyof LineupGameLog,
                          'defFg3Made' as keyof LineupGameLog, 'defFg3Att' as keyof LineupGameLog, 10),
  } : null;

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-card modal-xl" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <h3 className="modal-title">{lineupName}</h3>
          <button className="modal-close" onClick={onClose}>&times;</button>
        </div>
        <div className="modal-body">
          {loading && <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-muted)' }}>Loading...</div>}
          {error && <div style={{ padding: 24, textAlign: 'center', color: 'var(--negative)' }}>Failed: {error}</div>}
          {!loading && !error && games.length === 0 && (
            <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-muted)' }}>No games found for this lineup.</div>
          )}
          {!loading && !error && games.length > 0 && (
            <div className="data-table-wrap" style={{ maxHeight: '60vh' }}>
              <table className="data-table modal-table">
                <thead>
                  {viewMode === 'ff' ? (
                    <>
                      <tr>
                        <th colSpan={5} className="group-empty"></th>
                        <th colSpan={6} className="section-start">OFFENSE</th>
                        <th colSpan={6} className="section-start">DEFENSE</th>
                        <th colSpan={1} className="section-start"></th>
                      </tr>
                      <tr>
                        <th>GN</th><th>Date</th><th>Opponent</th><th>W/L</th><th>Score</th>
                        <th className="section-start">PPP</th><th>TS%</th><th>OREB%</th><th>TOV%</th><th>FTR</th><th>Poss</th>
                        <th className="section-start">PPP</th><th>TS%</th><th>OREB%</th><th>TOV%</th><th>FTR</th><th>Poss</th>
                        <th className="section-start">Min</th>
                      </tr>
                    </>
                  ) : (
                    <tr>
                      <th>GN</th><th>Date</th><th>Opponent</th><th>W/L</th><th>Score</th>
                      <th className="section-start">Off PPP</th><th>Def PPP</th><th>Net</th>
                      <th className="section-start" style={{ cursor: 'default' }}>Off Shot</th>
                      <th style={{ cursor: 'default' }}>Def Shot</th>
                      <th className="section-start">Off Poss</th><th>Def Poss</th><th>Min</th>
                    </tr>
                  )}
                </thead>
                <tbody>
                  {games.map((g, i) => (
                    <tr key={i}>
                      <td style={{ fontFamily: 'var(--font-mono)' }}>{g.gn ?? '-'}</td>
                      <td style={{ fontSize: 11 }}>{g.gameDate}</td>
                      <td style={{ textAlign: 'left', fontFamily: 'var(--font-sans)' }}>{g.opponent}</td>
                      <td>
                        <span className={`wl-badge ${g.result === 'W' ? 'wl-w' : 'wl-l'}`}>
                          {g.result}
                        </span>
                      </td>
                      <td style={{ fontFamily: 'var(--font-mono)', fontSize: 11 }}>{g.score}</td>
                      {viewMode === 'ff' ? (
                        <>
                          <td className="section-start">{fmtRate(g.offPpp)}</td>
                          <td>{fmtRate(g.offTs)}</td>
                          <td>{fmtRate(g.offOreb)}</td>
                          <td>{fmtRate(g.offTov)}</td>
                          <td>{fmtRate(g.offFtr)}</td>
                          <td style={{ color: 'var(--text-secondary)' }}>{g.offPoss?.toLocaleString()}</td>
                          <td className="section-start">{fmtRate(g.defPpp)}</td>
                          <td>{fmtRate(g.defTs)}</td>
                          <td>{fmtRate(g.defOreb)}</td>
                          <td>{fmtRate(g.defTov)}</td>
                          <td>{fmtRate(g.defFtr)}</td>
                          <td style={{ color: 'var(--text-secondary)' }}>{g.defPoss?.toLocaleString()}</td>
                          <td className="section-start" style={{ color: 'var(--text-muted)' }}>{fmtRate(g.minutes)}</td>
                        </>
                      ) : (
                        <>
                          <td className="section-start">{fmtRate(g.offPpp)}</td>
                          <td>{fmtRate(g.defPpp)}</td>
                          <td>{fmtNet(g.netRtg)}</td>
                          <ShotCell
                            fg2Made={g.offFg2Made ?? 0} fg2Att={g.offFg2Att ?? 0}
                            fg3Made={g.offFg3Made ?? 0} fg3Att={g.offFg3Att ?? 0}
                            avg2={shotAvgs?.off.avg2} avg3={shotAvgs?.off.avg3}
                            minFga={10} sectionStart
                          />
                          <ShotCell
                            fg2Made={g.defFg2Made ?? 0} fg2Att={g.defFg2Att ?? 0}
                            fg3Made={g.defFg3Made ?? 0} fg3Att={g.defFg3Att ?? 0}
                            avg2={shotAvgs?.def.avg2} avg3={shotAvgs?.def.avg3}
                            isDefense minFga={10}
                          />
                          <td className="section-start" style={{ color: 'var(--text-secondary)' }}>{g.offPoss?.toLocaleString()}</td>
                          <td style={{ color: 'var(--text-secondary)' }}>{g.defPoss?.toLocaleString()}</td>
                          <td style={{ color: 'var(--text-muted)' }}>{fmtRate(g.minutes)}</td>
                        </>
                      )}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function fmtRate(v: number | null | undefined): string {
  if (v == null || isNaN(v)) return '-';
  return v.toFixed(1);
}

function fmtNet(v: number | null | undefined): React.ReactNode {
  if (v == null || isNaN(v)) return '-';
  const s = v.toFixed(1);
  if (v > 0) return <span className="cell-pos">+{s}</span>;
  if (v < 0) return <span className="cell-neg">{s}</span>;
  return s;
}

