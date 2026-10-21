import { ParamError } from "./api";
import { parseContext } from "./contextParams";
import type { League } from "../rules/gameLogs";
export function parseLineups(s: URLSearchParams, league: League) {
  const c = parseContext(s, league);
  const integer = (k: string, fallback: number, min: number, max: number) => {
    const v = s.get(k) ?? String(fallback);
    if (!/^\d+$/.test(v) || Number(v) < min || Number(v) > max)
      throw new ParamError(k, `Expected ${min}-${max}`);
    return Number(v);
  };
  const view = s.get("view") ?? "summary";
  if (view !== "summary" && view !== "four_factors")
    throw new ParamError("view", "Invalid view");
  const status = s.get("clutch_status") ?? "all";
  if (!["all", "leading", "trailing", "tied"].includes(status))
    throw new ParamError("clutch_status", "Invalid score status");
  for (const k of ["clutch", "ot_margin"])
    if (s.has(k) && !["true", "false"].includes(s.get(k)!))
      throw new ParamError(k, "Expected true or false");
  const clutch = s.get("clutch") === "true";
  return {
    c,
    view,
    size: integer("size", 5, 2, 5),
    maxMargin: clutch ? integer("clutch_margin", 5, 0, 10) : null,
    marginStatus: clutch ? status : null,
    maxTime: clutch ? integer("clutch_minutes", 5, 1, 5) * 60 : null,
    otMargin: clutch && s.get("ot_margin") === "true",
  };
}
export type LineupParams = ReturnType<typeof parseLineups>;
