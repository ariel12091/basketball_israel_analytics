/** app/R/helpers.R: R round(), IEC 60559 ties to even, including binary input error. */
export function rRound(x: number, digits = 0): number {
  if (!Number.isFinite(x)) return x;
  if (x === 0) return 0;
  if (digits === 0) {
    const lower = Math.floor(x),
      distance = x - lower;
    return (
      (distance < 0.5 || (distance === 0.5 && lower % 2 === 0)
        ? lower
        : lower + 1) || 0
    );
  }
  // Compare distances in the original domain. Multiplication can turn a value
  // just below a decimal midpoint into an apparent exact tie.
  const sign = x < 0 ? -1 : 1,
    value = Math.abs(x);
  const scale = 10 ** digits,
    scaled = value * scale;
  if (!Number.isFinite(scaled)) return x;
  const integer = Math.floor(scaled),
    lower = integer / scale,
    upper = Math.ceil(scaled) / scale;
  const down = value - lower,
    up = upper - value;
  return (
    sign * (up < down || (up === down && integer % 2 !== 0) ? upper : lower) ||
    0
  );
}
