// The Team Ratings table's page state: what TeamRatingsClient, the API's
// shape=table (teamRatingsPageAnswer) and the chat pass to teamTableRows().
import type { Row } from "../../rules/gameLogs";
import { teamTableRows, type TeamView } from "../../rules/teamRatings";
import { parseStats, type Query } from "../tabQuery";

/** shape=table: the answer with its rows replaced by the page's table. */
export function teamRatingsTable<
  A extends { rows: Row[]; previous?: Row[]; meta: { showDelta?: boolean } },
>(answer: A, query: Query) {
  const o = teamRatingsPageOptions(query);
  const rows = teamTableRows(
    answer.rows,
    answer.previous ?? [],
    o.view,
    o.filters,
    answer.meta.showDelta ?? false,
    o.defense,
  );
  return { ...answer, rows, meta: { ...answer.meta, shape: "table" as const } };
}

export const TEAM_VIEWS: TeamView[] = ["summary", "four_factors", "shot_profile", "traditional"];

export function teamRatingsPageOptions(query: Query) {
  return {
    view: (TEAM_VIEWS.includes(query.view as TeamView) ? query.view : "summary") as TeamView,
    defense: query.side === "defense",
    filters: parseStats(query.stats ?? ""),
  };
}
