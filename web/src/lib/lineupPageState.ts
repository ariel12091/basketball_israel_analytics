// The Lineups table's page state as lineupsPagePath (tabQuery.ts) writes it:
// read once here for the server (lineupsPageAnswer) and the browser tests'
// substituted answers, so both apply lineupPage to the same options.
import { playerIds, type LineupView } from "../rules/lineups";
import { parseStats } from "./tabQuery";
import ui from "../data/shinyUi.json";

export function lineupPageOptions(
  search: URLSearchParams,
  view: LineupView,
) {
  // A request that names neither starts as the page does: automatic
  // minimum. The page itself always sends one of the two.
  const auto =
    search.get("auto") === "true" ||
    (search.get("auto") === null && search.get("min_poss") === null);
  return {
    team: search.get("team") ?? "",
    selection: {
      on: playerIds(search.get("players_on")),
      any: playerIds(search.get("players_any")),
      off: playerIds(search.get("players_off")),
      count: Number(search.get("any_count")) || 1,
      exact: search.get("any_exact") === "true",
    },
    view,
    min: auto
      ? null
      : Number(
          search.get("min_poss") ?? ui.lineupDefaults.minPoss,
        ),
    stats: parseStats(search.get("stats") ?? ""),
  };
}
