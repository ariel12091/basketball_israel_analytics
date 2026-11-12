/**
 * The order of the names in a lineup (docs/shared_definitions.md, "Lineup
 * name order"): shortest to tallest by basket.co.il's height, a tie broken by
 * position (guards first) and then by name, a player with no height last
 * (user 2026-11-12). Display only. Gameflow (`bySize`) and every lineup list
 * (`applyPlayerNames`) call this one comparison.
 */
export type PlayerSize = { height: number | null; position: string | null };

/** basket.co.il's position codes, guards first. */
const POSITION_RANK: Record<string, number> = { PG: 1, G: 2, "G-F": 3, F: 4, "F-C": 5, C: 6 };

export function compareBySize(
  a: PlayerSize | undefined,
  b: PlayerSize | undefined,
  labelA: string,
  labelB: string,
): number {
  const ha = a?.height ?? Infinity,
    hb = b?.height ?? Infinity;
  const pa = POSITION_RANK[a?.position ?? ""] ?? 9,
    pb = POSITION_RANK[b?.position ?? ""] ?? 9;
  return (ha === hb ? 0 : ha < hb ? -1 : 1) || pa - pb || labelA.localeCompare(labelB);
}

/** A lineup's members in size order -- or, when nobody in it has a height
 * (EuroLeague, players basket.co.il does not measure), in their own order. */
export function orderBySize<T>(
  members: T[],
  size: (m: T) => PlayerSize | undefined,
  label: (m: T) => string,
): T[] {
  if (!members.some((m) => size(m)?.height != null)) return members;
  return [...members].sort((a, b) => compareBySize(size(a), size(b), label(a), label(b)));
}
