interface ShotSplitCellProps {
  fg2Pct: number;
  fg3Pct: number;
  fg2Acc: number;
  fg3Acc: number;
}

function clamp(value: number): number {
  return Math.max(0, Math.min(100, value));
}

export function ShotSplitCell({ fg2Pct, fg3Pct, fg2Acc, fg3Acc }: ShotSplitCellProps) {
  const w2 = clamp(fg2Pct);
  const w3 = clamp(fg3Pct);

  return (
    <div className="shot-cell">
      <div className="shot-top">
        <span className="shot-acc-2">{fg2Acc.toFixed(0)}%</span>
        <span className="shot-sep">|</span>
        <span className="shot-acc-3">{fg3Acc.toFixed(0)}%</span>
      </div>
      <div className="shot-bar" aria-label="Shot frequency bar">
        <span className="shot-bar-2" style={{ width: `${w2}%` }}>
          {w2 > 18 ? `${w2.toFixed(0)}%` : ''}
        </span>
        <span className="shot-bar-3" style={{ width: `${w3}%` }}>
          {w3 > 18 ? `${w3.toFixed(0)}%` : ''}
        </span>
      </div>
    </div>
  );
}
