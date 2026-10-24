/** DTWidget.formatRound / formatCurrency: toFixed, then "," every 3 digits. */
export function formatNumber(v: unknown, digits: number): string {
  if (v == null || v === "") return "";
  const n = Number(v);
  if (!Number.isFinite(n)) return String(v);
  const [int, dec] = n.toFixed(digits).split(".");
  const s = int.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  return dec != null ? `${s}.${dec}` : s;
}
