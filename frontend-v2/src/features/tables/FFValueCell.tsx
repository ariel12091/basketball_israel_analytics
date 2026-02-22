interface FFValueCellProps {
  value: number;
  good?: 'high' | 'low'; // 'high' means higher is better
  best?: number;
  worst?: number;
  sectionStart?: boolean;
}

export default function FFValueCell({
  value,
  good = 'high',
  best = 60,
  worst = 45,
  sectionStart,
}: FFValueCellProps) {
  const range = best - worst;
  let pct = (value - worst) / range;
  pct = Math.max(0, Math.min(1, pct));
  if (good === 'low') pct = 1 - pct;

  const color =
    pct > 0.65
      ? 'var(--positive)'
      : pct < 0.35
        ? 'var(--negative)'
        : 'var(--text-secondary)';

  return (
    <td className={sectionStart ? 'section-start' : ''}>
      <span style={{ color, fontWeight: 600 }}>{value.toFixed(1)}</span>
    </td>
  );
}
