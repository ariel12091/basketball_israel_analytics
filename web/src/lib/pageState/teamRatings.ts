// The Team Ratings table's page state: what TeamRatingsClient, the API's
// shape=table (teamRatingsPageAnswer) and the chat pass to teamTableRows().
import type { Row } from "../../rules/gameLogs";
import { teamTableRows, type TeamView } from "../../rules/teamRatings";
import { viewsForFilters, withOtherView } from "../../rules/statFilters";
import { parseStats, type Query } from "../tabQuery";
import ui from "../../data/shinyUi.json";

/** shape=table: the answer with its rows replaced by the page's table. */
export function teamRatingsTable<
  A extends { rows: Row[]; previous?: Row[]; meta: { showDelta?: boolean } },
>(answer: A, query: Query, others: Row[][] = []) {
  const o = teamRatingsPageOptions(query);
  const rows = teamTableRows(
    teamWithOtherViews(answer.rows, others),
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

const TEAM_FILTER_COLS = Object.fromEntries(
  TEAM_VIEWS.map((v) => [v, ui.teamStatFilterCols[v].map((c) => c.col)]),
) as Record<TeamView, string[]>;
/** The other views whose columns the stat filters name (Four Factors
 * filtered by Traditional's 3P%, say); the page reads each for the same
 * context and joins it by team. */
export function teamOtherViews(query: Query): TeamView[] {
  const o = teamRatingsPageOptions(query);
  return viewsForFilters(o.filters, TEAM_FILTER_COLS, o.view);
}
/** The same query on another view, for teamRatingsPath. */
export const teamViewQuery = (query: Query, view: TeamView): Query => ({
  ...query,
  view,
});
export function teamWithOtherViews(rows: Row[], others: Row[][]): Row[] {
  return others.reduce(
    (acc, other) => withOtherView(acc, other, (r) => String(r.team_id)),
    rows,
  );
}
