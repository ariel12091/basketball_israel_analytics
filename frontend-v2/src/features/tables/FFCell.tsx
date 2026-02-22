import { heatClass } from './HeatCell';

interface FFCellProps {
  diff: number;
  onVal: number;
  offVal: number;
  onRank: number | null; // 0-100 percentile, null = unranked
  offRank: number | null;
  sectionStart?: boolean;
  /** If true, negative diff = good (green). Use for defense TS%/OREB%/FTR and offense TOV%. */
  invert?: boolean;
  /** Percentile rank 0-1 for background heat color. null = no background. */
  heatPr?: number | null;
}

export default function FFCell({
  diff,
  onVal,
  offVal,
  onRank,
  offRank,
  sectionStart,
  invert = false,
  heatPr,
}: FFCellProps) {
  const unranked = onRank === null || offRank === null;

  // Background heat class (from diff rank)
  const bgClass = heatPr != null ? heatClass(heatPr) : '';

  const cls = [
    bgClass,
    sectionStart ? 'section-start' : '',
  ].filter(Boolean).join(' ');

  const diffStr = diff > 0 ? `+${diff.toFixed(1)}%` : `${diff.toFixed(1)}%`;

  const isGood = invert ? diff < 0 : diff > 0;
  const isBad = invert ? diff > 0 : diff < 0;

  // Unranked: gray text. Ranked: green/red/neutral.
  const diffColor = unranked
    ? 'var(--text-muted)'
    : isGood ? 'var(--positive)' : isBad ? 'var(--negative)' : 'var(--text-secondary)';

  const safeOn = onRank ?? 50;
  const safeOff = offRank ?? 50;
  const left = Math.min(safeOn, safeOff);
  const width = Math.abs(safeOn - safeOff);

  return (
    <td className={cls}>
      <div className="ff-cell">
        <div className={`ff-diff${unranked ? ' unranked' : ''}`} style={{ color: diffColor }}>
          {diffStr}
        </div>
        {unranked ? (
          <div className="ff-track" style={{ display: 'none' }} />
        ) : (
          <div className="ff-track">
            <div
              className="ff-connect"
              style={{ left: `${left}%`, width: `${width}%` }}
            />
            <div className="ff-dot-off" style={{ left: `${safeOff}%` }} />
            <div className="ff-dot-on" style={{ left: `${safeOn}%` }} />
          </div>
        )}
        <div className="ff-sub" style={unranked ? { opacity: 0.5 } : undefined}>
          <span className="on-val">{(onVal * 100).toFixed(1)}%</span>
          <span style={{ opacity: 0.4 }}>|</span>
          {(offVal * 100).toFixed(1)}%
        </div>
      </div>
    </td>
  );
}
