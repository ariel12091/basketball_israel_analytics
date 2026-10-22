// Uses the authenticated CLI; arguments go directly to Node to preserve '&' on Windows.
import { execFile, execFileSync } from "node:child_process";
import { promisify } from "node:util";
import { setTimeout } from "node:timers/promises";
import path from "node:path";
import { readFileSync } from "node:fs";
const deployment = process.argv[2];
if (!/^https:\/\/[a-z0-9-]+\.vercel\.app$/.test(deployment ?? ""))
  throw new Error("Expected a Vercel deployment URL");
const command = execFileSync("where.exe", ["vercel.cmd"], { encoding: "utf8" })
  .trim()
  .split(/\r?\n/)[0];
const packageDir = path.join(path.dirname(command), "node_modules/vercel");
const cli = path.join(
  packageDir,
  JSON.parse(readFileSync(path.join(packageDir, "package.json"), "utf8")).bin
    .vercel,
);
const endpoints = [
  "/api/v1/il/game-logs?season=2026",
  "/api/v1/il/game-logs?season=2026&view=four_factors",
  "/api/v1/euroleague/game-logs?season=2025",
  "/api/v1/euroleague/game-logs?season=2025&view=four_factors",
  "/api/v1/eurocup/game-logs?season=2025",
  "/api/v1/il/onoff?season=2026",
  "/api/v1/il/onoff?season=2026&view=four_factors",
  "/api/v1/il/onoff?season=2026&view=shot_profile",
  "/api/v1/euroleague/onoff?season=2025",
  "/api/v1/euroleague/onoff?season=2025&view=four_factors",
  "/api/v1/eurocup/onoff?season=2025",
  "/api/v1/eurocup/onoff?season=2025&view=four_factors",
  "/api/v1/il/onoff?season=2026&home_away=home",
  "/api/v1/euroleague/onoff?season=2025&view=four_factors&home_away=home",
  "/api/v1/il/player-stat-filters?season=2026",
  "/api/v1/version",
];
async function request(endpoint) {
  try {
    const { stdout } = await promisify(execFile)(
      process.execPath,
      [cli, "curl", endpoint, "--deployment", deployment, "--", "--silent"],
      { maxBuffer: 32 * 1024 * 1024 },
    );
    return stdout;
  } catch (error) {
    // execFile errors carry the response body; keep verification logs compact.
    throw new Error(
      `Preview request failed: ${endpoint} (${error.code ?? "CLI error"})`,
    );
  }
}
for (const [league, season] of [
  ["il", 2026],
  ["euroleague", 2025],
  ["eurocup", 2025],
]) {
  endpoints.push(
    `/api/v1/${league}/lineups?season=${season}`,
    `/api/v1/${league}/team-ratings?season=${season}`,
  );
  for (const context of [
    "",
    "&last_n=5&home_away=home",
    "&clutch=true",
    "&clutch=true&clutch_margin=3&clutch_status=trailing&clutch_minutes=2&ot_margin=true",
  ])
    endpoints.push(`/api/v1/${league}/player-stats?season=${season}${context}`);
}
for (const endpoint of endpoints) {
  let passed = false;
  for (let attempt = 1; attempt <= 3; attempt++) {
    const stdout = await request(endpoint);
    const answer = JSON.parse(stdout);
    console.log(
      JSON.stringify({
        endpoint,
        attempt,
        rows: answer.rows?.length,
        leagues: answer.leagues?.length,
        error: answer.error ?? null,
      }),
    );
    if (
      !answer.error &&
      (Array.isArray(answer.rows) || Array.isArray(answer.leagues))
    ) {
      passed = true;
      break;
    }
    await setTimeout(500);
  }
  if (!passed) throw new Error("Preview verification failed: " + endpoint);
}
for (const [league, season] of [
  ["il", 2026],
  ["euroleague", 2025],
  ["eurocup", 2025],
]) {
  const endpoint = `/${league}/player-stats?season=${season}`;
  const stdout = await request(endpoint);
  if (!stdout.includes('data-column="Player"'))
    throw new Error(`Missing initial Player Stats table: ${league}`);
  console.log(JSON.stringify({ endpoint, serverRendered: true }));
}
