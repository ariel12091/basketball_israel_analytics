interface ShotCellProps {
  fg2Made: number;
  fg2Att: number;
  fg3Made: number;
  fg3Att: number;
  avg2?: number;
  avg3?: number;
  /** If true, lower opponent FG% = green (defense columns). */
  isDefense?: boolean;
  /** Minimum total FGA to show colored text (below = muted gray). */
  minFga?: number;
  sectionStart?: boolean;
}

/**
 * Continuous RGB gradient for accuracy text, matching Shiny's accColor().
 * sign = 1 for offense (higher pct = green), -1 for defense (lower pct = green).
 * Normalizes diff by avg, amplifies 3x, clamps to [-1, 1].
 */
function accColor(pct: number, avg: number, sign: number): string {
  let d = (sign * (pct - avg)) / avg;
  d = Math.max(-1, Math.min(1, d * 3));
  let r: number, g: number;
  if (d < 0) {
    r = 200;
    g = Math.round(200 + d * 120);
  } else {
    g = 170;
    r = Math.round(200 - d * 150);
  }
  return `rgb(${r},${g},60)`;
}

export default function ShotCell({
  fg2Made,
  fg2Att,
  fg3Made,
  fg3Att,
  avg2 = 53,
  avg3 = 34,
  isDefense = false,
  minFga = 50,
  sectionStart,
}: ShotCellProps) {
  const totalAtt = fg2Att + fg3Att;
  if (totalAtt === 0) {
    return (
      <td className={sectionStart ? 'section-start' : ''}>
        <div className="shot-acc" style={{ color: '#aaa' }}>-</div>
      </td>
    );
  }

  const fg2Freq = Math.round((fg2Att / totalAtt) * 100);
  const fg3Freq = 100 - fg2Freq;
  const fg2Pct = fg2Att > 0 ? Math.round((fg2Made / fg2Att) * 100) : 0;
  const fg3Pct = fg3Att > 0 ? Math.round((fg3Made / fg3Att) * 100) : 0;

  const muted = totalAtt < minFga;
  const sign = isDefense ? -1 : 1;
  const c2 = muted ? '#bbb' : accColor(fg2Pct, avg2, sign);
  const c3 = muted ? '#bbb' : accColor(fg3Pct, avg3, sign);

  return (
    <td className={sectionStart ? 'section-start' : ''}>
      <div className="shot-cell">
        <div className="shot-acc">
          <span style={{ color: c2, fontWeight: muted ? 400 : 700 }}>{fg2Pct}%</span>
          <span style={{ opacity: 0.3, margin: '0 2px' }}>|</span>
          <span style={{ color: c3, fontWeight: muted ? 400 : 700 }}>{fg3Pct}%</span>
        </div>
        <div className="shot-bar" style={muted ? { opacity: 0.3 } : undefined}>
          <div className="shot-bar-2" style={{ width: `${fg2Freq}%` }}>
            {fg2Freq}
          </div>
          <div className="shot-bar-3" style={{ width: `${fg3Freq}%` }}>
            {fg3Freq}
          </div>
        </div>
      </div>
    </td>
  );
}
