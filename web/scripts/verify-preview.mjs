// Uses the authenticated CLI; arguments go directly to Node to preserve '&' on Windows.
import { execFile, execFileSync } from "node:child_process";
import { promisify } from "node:util";
import { setTimeout } from "node:timers/promises";
import path from "node:path";
const deployment = process.argv[2];
if (!/^https:\/\/[a-z0-9-]+\.vercel\.app$/.test(deployment ?? ""))
  throw new Error("Expected a Vercel deployment URL");
const command = execFileSync("where.exe", ["vercel.cmd"], { encoding: "utf8" })
  .trim()
  .split(/\r?\n/)[0];
const cli = path.join(
  path.dirname(command),
  "node_modules/vercel/dist/index.js",
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
for (const endpoint of endpoints) {
  let passed = false;
  for (let attempt = 1; attempt <= 3; attempt++) {
    const { stdout } = await promisify(execFile)(
      process.execPath,
      [cli, "curl", endpoint, "--deployment", deployment, "--", "--silent"],
      { maxBuffer: 5 * 1024 * 1024 },
    );
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
