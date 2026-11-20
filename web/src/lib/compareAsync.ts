import type { Row } from "../rules/gameLogs";

type ReadState = {
  data?: { meta: { version: string } };
  isFetching: boolean;
  isError: boolean;
};

/** A cached answer undergoing refresh is not a completed current answer. */
export function comparePairReady(a: ReadState, b: ReadState, version?: string) {
  return (
    !!a.data &&
    !!b.data &&
    !a.isFetching &&
    !b.isFetching &&
    !a.isError &&
    !b.isError &&
    a.data.meta.version === b.data.meta.version &&
    (version === undefined || a.data.meta.version === version)
  );
}

/** The two players Players Compare opens on: the season's top two scorers by
 * points per game (ties: more points) among players with at least half the
 * games of the season's most-used player, so one big game never wins (a
 * 1-game player led 2026-27 on 2026-11-20). Shiny sampled two of the top
 * three by PPG with no games minimum; the web picks the same pair every load,
 * so a shared link and a fresh visit agree. Fewer than two candidates: the
 * first two players of the picker. */
export function compareDefaultPlayers(rows: Row[], validIds: number[]) {
  const valid = new Set(validIds);
  const scoring = rows.filter(
    (r) => Number(r.gp) > 0 && r.pts != null && valid.has(Number(r.player_id)),
  );
  const most = Math.max(0, ...scoring.map((r) => Number(r.gp)));
  let ids = [
    ...new Set(
      scoring
        .filter((r) => Number(r.gp) >= Math.max(1, most / 2))
        .sort(
          (a, b) =>
            Number(b.pts) / Number(b.gp) - Number(a.pts) / Number(a.gp) ||
            Number(b.pts) - Number(a.pts),
        )
        .map((r) => Number(r.player_id)),
    ),
  ].slice(0, 2);
  if (ids.length < 2) ids = [...valid].slice(0, 2);
  return ids;
}
