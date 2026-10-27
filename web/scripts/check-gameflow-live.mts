import { deepStrictEqual, equal } from "node:assert/strict";
import goldens from "../test/gameflowGoldens";
const { gameflowFiles, loadGameflow } = goldens;
const base = process.env.GAMEFLOW_BASE_URL ?? "http://localhost:3100";
for (const file of gameflowFiles) {
  const { g, answer, url } = loadGameflow(file);
  const response = await fetch(`${base}/api/v1${url}`, {
    signal: AbortSignal.timeout(35000),
  });
  equal(response.status, 200, file);
  const actual = await response.json();
  for (const key of [
    "lanes",
    "bars",
    "margin",
    "dict",
    "totals",
    "details",
    "health",
  ] as const)
    deepStrictEqual(
      actual[key],
      JSON.parse(JSON.stringify(g.expected[key])),
      `${file}: ${key}`,
    );
  equal(actual.meta.ownTeam, answer.meta.ownTeam);
  console.log(`${file}: live reader matches Shiny`);
}
for (const query of [
  "game_id=0&team_id=1",
  "game_id=390&team_id=-1",
  "game_id=390&team_id=999999",
]) {
  equal((await fetch(`${base}/api/v1/il/gameflow?${query}`)).status, 400);
}
console.log("Invalid IDs and game/team mismatch rejected");
