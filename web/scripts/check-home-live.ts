// Sequential local production smoke, never a concurrency/capacity benchmark.
import { readFileSync, mkdirSync, writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { homeDashboard, type HomeFrames } from "../src/rules/home";
async function main() {
  const base = process.argv[2] ?? "http://localhost:3100";
  if (!["localhost", "127.0.0.1"].includes(new URL(base).hostname))
    throw Error("Local smoke only");
  const goldens = JSON.parse(
    readFileSync("test/goldens/home/cases.json", "utf8"),
  );
  const results: {
    name: string;
    status: number;
    ms: number;
    ok: boolean;
    error?: string;
  }[] = [];
  async function check(
    name: string,
    path: string,
    verify: (res: Response, text: string) => void,
    redirect: RequestRedirect = "follow",
  ) {
    const start = performance.now();
    let status = 0;
    try {
      const res = await fetch(new URL(path, base), {
        redirect,
        signal: AbortSignal.timeout(30000),
      });
      status = res.status;
      verify(res, await res.text());
      results.push({ name, status, ms: performance.now() - start, ok: true });
    } catch (e) {
      results.push({
        name,
        status,
        ms: performance.now() - start,
        ok: false,
        error: String(e),
      });
    }
    console.log(JSON.stringify(results.at(-1)));
  }
  const assert = (ok: unknown, message: string) => {
    if (!ok) throw Error(message);
  };
  function equivalent(actual: unknown, expected: unknown, path = "") {
    if (typeof expected === "number")
      assert(
        typeof actual === "number" &&
          Math.abs(actual - expected) < 1e-8 * Math.max(1, Math.abs(expected)),
        `Mismatch ${path}`,
      );
    else if (Array.isArray(expected)) {
      assert(
        Array.isArray(actual) && actual.length === expected.length,
        `Length ${path}`,
      );
      expected.forEach((v, i) =>
        equivalent((actual as unknown[])[i], v, `${path}.${i}`),
      );
    } else if (expected && typeof expected === "object")
      for (const [k, v] of Object.entries(expected))
        equivalent((actual as Record<string, unknown>)?.[k], v, `${path}.${k}`);
    else assert(actual === expected, `Mismatch ${path}`);
  }
  for (const season of ["2026", "2027"])
    for (const team of ["2", "5"])
      await check(
        `API ${season}/${team}`,
        `/api/v1/il/home?season=${season}&team_id=${team}`,
        (res, text) => {
          const a = JSON.parse(text);
          assert(res.ok, "API status");
          assert(
            a.meta?.season === season && a.meta?.teamId === team,
            "Response context",
          );
          assert(a.meta.errors.length === 0, "Unavailable section");
          assert(
            typeof a.meta.version === "string" && a.meta.version.length,
            "Version",
          );
          assert(
            a.frames.ratings.length > 0 && a.frames.four_factors.length > 0,
            "League frames",
          );
          if (season === "2026")
            equivalent(
              homeDashboard(a.frames as HomeFrames, team),
              goldens[`${season}_${team}`].expected,
            );
        },
      );
  for (const [league, season, team] of [
    ["il", "2026", "2"],
    ["il", "2026", "5"],
    ["il", "2027", "2"],
    ["euroleague", "2025", ""],
    ["eurocup", "2025", ""],
  ])
    await check(
      `SSR ${league}/${season}/${team}`,
      `/${league}/home?season=${season}${team ? `&team_id=${team}` : ""}`,
      (res, text) => {
        assert(res.ok, "SSR status");
        assert(
          (text.match(/class="home-nav-card"/g) ?? []).length ===
            (league === "il" ? 6 : 5),
          "Server-rendered cards",
        );
        if (league === "il")
          assert(
            text.includes(
              String(
                goldens[`${season}_${team}`].expected.identity.row.team_name,
              ),
            ),
            "Server-rendered identity",
          );
        else
          assert(
            !text.includes('id="team_hub_section"'),
            "Euro Home must not render Israeli hub",
          );
      },
    );
  await check(
    "All teams shell",
    "/il/home?season=2026&team_id=all",
    (res, text) => {
      assert(res.ok, "Shell status");
      assert(!text.includes('id="team_hub_section"'), "All teams hub");
    },
  );
  await check(
    "Root Home redirect",
    "/",
    (res) => {
      assert(res.status === 307, "Redirect status");
      assert(res.headers.get("location") === "/il/home", "Home location");
    },
    "manual",
  );
  for (const [name, path] of [
    [
      "Euro dashboard rejection",
      "/api/v1/euroleague/home?season=2025&team_id=2",
    ],
    ["Invalid team", "/api/v1/il/home?season=2026&team_id=2%3BDROP"],
    ["Invalid season", "/api/v1/il/home?season=1900&team_id=2"],
    ["Repeated parameter", "/api/v1/il/home?season=2026&team_id=2&team_id=5"],
  ])
    await check(name, path, (res) => {
      assert(res.status === 400, "Expected validation rejection");
      assert(
        res.headers.get("Cache-Control") === "no-store",
        "Error must not be cached",
      );
    });
  mkdirSync("../output/home", { recursive: true });
  writeFileSync(
    "../output/home/live.json",
    JSON.stringify(
      {
        timestamp: new Date().toISOString(),
        scope: "Single-user local production smoke; no load/capacity claim",
        results,
      },
      null,
      2,
    ),
  );
  if (results.some((r) => !r.ok)) process.exitCode = 1;
}
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
