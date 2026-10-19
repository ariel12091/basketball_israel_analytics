interface PercentileTrackProps {
  diff?: number;
  onValue?: number;
  offValue?: number;
  onRank?: number;
  offRank?: number;
  diffRank?: number;
  polarity?: 'good-high' | 'good-low';
  decimals?: number;
}

function clamp(value: number): number {
  return Math.max(0, Math.min(100, value));
}

function formatSigned(value?: number, decimals = 1): string {
  if (value === undefined || Number.isNaN(value)) return '-';
  const formatted = value.toFixed(decimals);
  return value > 0 ? `+${formatted}` : formatted;
}

function formatValue(value?: number, decimals = 1): string {
  if (value === undefined || Number.isNaN(value)) return '-';
  return value.toFixed(decimals);
}

export function PercentileTrack({
  diff,
  onValue,
  offValue,
  onRank,
  offRank,
  diffRank,
  polarity = 'good-high',
  decimals = 1,
}: PercentileTrackProps) {
  const hasRanks = Number.isFinite(onRank) && Number.isFinite(offRank);
  const onPct = hasRanks ? clamp(onRank as number) : 0;
  const offPct = hasRanks ? clamp(offRank as number) : 0;
  const left = Math.min(onPct, offPct);
  const width = Math.abs(onPct - offPct);
  const rankScale = Number.isFinite(diffRank)
    ? clamp((diffRank as number) <= 1 ? (diffRank as number) * 100 : (diffRank as number))
    : undefined;
  const goodPct = rankScale === undefined
    ? undefined
    : polarity === 'good-high'
      ? rankScale
      : 100 - rankScale;
  const green = { r: 22, g: 176, b: 92 };
  const orange = { r: 210, g: 122, b: 53 };
  const mix = goodPct === undefined ? undefined : goodPct / 100;
  const r = mix === undefined ? 247 : Math.round(orange.r + (green.r - orange.r) * mix);
  const g = mix === undefined ? 247 : Math.round(orange.g + (green.g - orange.g) * mix);
  const b = mix === undefined ? 247 : Math.round(orange.b + (green.b - orange.b) * mix);
  const tint = `rgba(${r}, ${g}, ${b}, ${mix === undefined ? 0.1 : 0.2})`;

  return (
    <div className="ff-range-cell" style={{ background: tint }}>
      <div className={`ff-diff-val ${hasRanks ? '' : 'unranked'}`}>{formatSigned(diff, decimals)}</div>
      {hasRanks ? (
        <div className="ff-rank-bar-container" aria-label="On vs off percentile range">
          <div className="ff-range-connect" style={{ left: `${left}%`, width: `${width}%` }} />
          <div className="ff-dot-off" style={{ left: `${offPct}%` }} title={`Off: ${formatValue(offValue, decimals)}`} />
          <div className="ff-dot-on" style={{ left: `${onPct}%` }} title={`On: ${formatValue(onValue, decimals)}`} />
        </div>
      ) : (
        <div className="ff-rank-bar-container hidden" />
      )}
      <div className="ff-sub-text">
        <span className="ff-sub-on">{formatValue(onValue, decimals)}</span>
        <span className="ff-sub-sep">|</span>
        <span className="ff-sub-off">{formatValue(offValue, decimals)}</span>
      </div>
    </div>
  );
}
