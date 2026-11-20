// The luck tag shared by Team Ratings, Lineups and On/Off
// (docs/shared_definitions.md, "Shooting luck (Team Ratings)" and the 3PT
// luck rows). `luck` is what shooting added to the Net: net - net without
// luck, per 100 possessions. On every tab a higher Net is better.

/** Team Ratings tags luck of at least this share of the Net... */
export const LUCK_SHARE = 0.5;
/** ...and at least this many points per 100 (near a Net of zero any luck is
 * a big share). A sign flip is tagged regardless (user 2026-11-20). */
export const LUCK_FLOOR = 1.5;

/** The arrow points to where the Net would be without luck. */
export function luckTag(luck: number): string {
  return luck < 0 ? "▲ Unlucky" : "▼ Lucky";
}

/** Whether the luck-free Net is better or worse than the one shown. */
export function luckSide(luck: number): "better" | "worse" {
  return luck < 0 ? "better" : "worse";
}

/** Team Ratings: the Net changes sign without the luck, or the luck is at
 * least half the Net and at least 1.5 per 100. */
export function teamLuckTagged(
  net: number | null,
  luck: number | null,
): boolean {
  if (net == null || luck == null) return false;
  const without = net - luck;
  if (net !== 0 && without * net <= 0) return true;
  return (
    Math.abs(luck) >= LUCK_FLOOR &&
    (net === 0 || Math.abs(luck) >= LUCK_SHARE * Math.abs(net))
  );
}
