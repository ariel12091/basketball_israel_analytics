/** app/R/helpers.R: R round(), IEC 60559 ties to even, including binary input error. */
export function rRound(x: number, digits = 0): number {
  if (!Number.isFinite(x)) return x;
  const scale = 10 ** digits;
  const floor = Math.floor(x * scale), fraction = x * scale - floor;
  const value = fraction < .5 ? floor : fraction > .5 ? floor + 1 : floor % 2 === 0 ? floor : floor + 1;
  return value / scale || 0;
}
