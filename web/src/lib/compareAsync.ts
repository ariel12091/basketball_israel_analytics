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

/** Shiny: take the top three scoring rows before removing duplicate IDs. */
export function compareDefaultPlayers(
  rows: Row[],
  validIds: number[],
  random = Math.random,
) {
  const valid = new Set(validIds);
  let ids = [
    ...new Set(
      rows
        .filter((r) => Number(r.gp) > 0 && r.pts != null)
        .sort(
          (a, b) =>
            Number(b.pts) / Number(b.gp) - Number(a.pts) / Number(a.gp) ||
            Number(b.pts) - Number(a.pts),
        )
        .slice(0, 3)
        .map((r) => Number(r.player_id)),
    ),
  ].filter((id) => valid.has(id));
  if (ids.length < 2) ids = [...valid].slice(0, 2);
  for (let i = ids.length - 1; i > 0; i--) {
    const j = Math.floor(random() * (i + 1));
    [ids[i], ids[j]] = [ids[j], ids[i]];
  }
  return ids.slice(0, 2);
}
