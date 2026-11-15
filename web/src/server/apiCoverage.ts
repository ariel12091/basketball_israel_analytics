// Inventory features (docs/superpowers/specs/2026-10-19-api-mcp-feature-
// inventory.md) the /api/v1 routes serve beyond each Ask tool's own list
// (ask/registry.ts `inventory`), and the ones that are page chrome with no
// data for an API to return. apiCoverage.test.ts requires every inventory row
// to be in exactly one place.

/** Served by the routes, grouped by where. */
export const API_COVERAGE: Record<string, string[]> = {
  /** Every route: the league path segment, each league's season convention
   * and date bounds (server/contextParams.ts), the clutch filter, the fast
   * and filtered paths behind one answer, the data version (`v`, /version),
   * the request guards (rawQueryGuard, deadlines, the snapshot queue), and
   * the display-name owner. */
  allRoutes: ["X-01", "X-02", "X-03", "X-06", "X-07", "X-08", "X-20", "X-21"],
  /** shape=table: the stat filter applied on the server (lib/pageState/). */
  tableShape: ["X-09"],
  /** format=csv on every table (server/csvAnswer.ts). */
  csv: ["X-14", "TR-11", "GL-08"],
};

/** Page chrome: what the page draws or remembers, not data. */
export const CLIENT_ONLY: Record<string, string> = {
  "X-10": "The filter chips bar is the page's view of its own query; the API takes the same query.",
  "X-12": "Heat colours are drawn by the page from the pr_* percentiles the API returns.",
  "X-13": "Default sort and sort sequences are table behaviour; callers sort the returned rows.",
  "X-15": "Column order is a per-browser preference kept in localStorage.",
  "X-17": "Row pivots are links between pages; each target page has its own route.",
  "X-18": "URL bookmarks are the page's address; every route already takes the same query.",
  "X-19": "The navbar stamp is page chrome; the data version itself is served (X-08).",
  "HOME-08": "Question cards are navigation into tabs, not data.",
  "HOME-09": "League chooser cards are navigation, not data.",
};
