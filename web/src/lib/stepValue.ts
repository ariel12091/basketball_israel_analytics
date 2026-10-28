/** A possession minimum typed or stepped by hand: rounded to the nearest
 * step, then clamped to the range; null when it is not a number. */
export function stepValue(
  raw: string | number,
  min: number,
  max: number,
  step: number,
): number | null {
  const text = typeof raw === "number" ? String(raw) : raw.trim();
  if (text === "") return null;
  const v = Number(text);
  if (!Number.isFinite(v)) return null;
  return Math.min(max, Math.max(min, Math.round(v / step) * step));
}
