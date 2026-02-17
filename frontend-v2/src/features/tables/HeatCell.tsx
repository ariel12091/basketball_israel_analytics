interface HeatCellProps {
  value: number | null | undefined;
  pr: number | null | undefined; // percentile rank 0-1
  bold?: boolean;
  sectionStart?: boolean;
  /** If true, negative diff = good (green text). Use for defense columns. */
  invert?: boolean;
  /** 'diff' = +/- sign with 2dp, 'ppp' = plain 1dp, 'net' = +/- sign with 1dp. Default: 'diff' */
  format?: 'diff' | 'ppp' | 'net';
}

function heatClass(pr: number): string {
  if (pr >= 0.9) return 'heat-9';
  if (pr >= 0.8) return 'heat-8';
  if (pr >= 0.7) return 'heat-7';
  if (pr >= 0.55) return 'heat-6';
  if (pr >= 0.45) return 'heat-5';
  if (pr >= 0.35) return 'heat-4';
  if (pr >= 0.25) return 'heat-3';
  if (pr >= 0.15) return 'heat-2';
  return 'heat-1';
}

/** Format a diff value with +/- sign and color. If invert=true, negative=good (green). */
export function formatDiff(v: number | null | undefined, invert = false, dp = 2): React.ReactNode {
  if (v == null || Number.isNaN(v)) return <span>-</span>;
  const s = v.toFixed(dp);
  const posClass = invert ? 'cell-neg' : 'cell-pos';
  const negClass = invert ? 'cell-pos' : 'cell-neg';
  if (v > 0) return <span className={posClass}>+{s}</span>;
  if (v < 0) return <span className={negClass}>{s}</span>;
  return <span>{s}</span>;
}

export default function HeatCell({ value, pr, bold, sectionStart, invert = false, format = 'diff' }: HeatCellProps) {
  const safePr = (pr == null || Number.isNaN(pr)) ? 0.5 : pr;
  const safeValue = (value == null || Number.isNaN(value)) ? null : value;
  const cls = [
    heatClass(safePr),
    bold ? 'cell-bold' : '',
    sectionStart ? 'section-start' : '',
  ]
    .filter(Boolean)
    .join(' ');

  let content: React.ReactNode;
  if (format === 'ppp') {
    content = safeValue == null ? '-' : safeValue.toFixed(1);
  } else if (format === 'net') {
    content = formatDiff(safeValue, invert, 1);
  } else {
    content = formatDiff(safeValue, invert, 2);
  }

  return <td className={cls}>{content}</td>;
}

export { heatClass };
