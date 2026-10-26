// Closed-loop load test against a protected Vercel Preview (run by
// .github/workflows/web-preview-load.yml). Each virtual user keeps its own
// session cookie, picks seeded random actions and thinks one second between
// them; stages ramp 1 -> 150 users for STAGE_SECONDS each and stop at the
// first stage over the limits (docs/matched_local_load_protocol.md: >5% failed
// or p95 > 5 s). Read-only: GET pages and API reads, as a visitor would.
// API reads carry the data version as `v`, as the browser does
// (src/lib/dataVersion.ts), so shared filter states can come from Vercel's
// CDN; TAIL (default 0.3) of them ask a date window nobody asked before, which
// the CDN cannot have and the database must answer.
//   PREVIEW_URL, VERCEL_AUTOMATION_BYPASS_SECRET,
//   [STAGES, STAGE_SECONDS, OUT, TAIL]
import { appendFileSync, writeFileSync } from "node:fs";

const base = process.env.PREVIEW_URL;
const secret = process.env.VERCEL_AUTOMATION_BYPASS_SECRET;
if (!/^https:\/\/[a-z0-9-]+\.vercel\.app$/.test(base ?? "")) throw new Error("PREVIEW_URL");
if (!secret) throw new Error("VERCEL_AUTOMATION_BYPASS_SECRET");
const STAGES = (process.env.STAGES ?? "1,10,20,50,100,150").split(",").map(Number);
const STAGE_MS = Number(process.env.STAGE_SECONDS ?? 60) * 1000;
const OUT = process.env.OUT ?? "load-results.json";
const TAIL = Number(process.env.TAIL ?? 0.3);
const DEADLINE_MS = 30000, THINK_MS = 1000;

// Seeded (protocol seed 20261023), so every run asks the same sequence.
// mulberry32: 32-bit integer arithmetic throughout. (An LCG written with plain
// multiplication loses precision past 2^53 and cycled every 10,466 draws,
// repeating most "never asked" date windows.)
let seed = 20261023;
const rand = () => {
  seed = (seed + 0x6d2b79f5) | 0;
  let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
  t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
};
const pick = (xs) => xs[Math.floor(rand() * xs.length)];

const LEAGUES = { il: [2026, 2027], euroleague: [2025, 2026], eurocup: [2025, 2026] };
const READERS = ["game-logs", "onoff", "lineups", "team-ratings", "player-stats"];
const PAGES = ["home", "game-logs", "onoff", "lineups", "team-ratings", "player-stats"];
const CONTEXTS = ["", "&home_away=home", "&home_away=away", "&last_n=5", "&last_n=10", "&outcome=win"];
const VIEWS = { onoff: ["", "&view=four_factors", "&view=shot_profile"], "team-ratings": ["", "&view=four_factors"], "game-logs": ["", "&view=four_factors"] };

function action() {
  const league = pick(Object.keys(LEAGUES)), season = pick(LEAGUES[league]);
  const r = rand();
  if (r < 0.1) return { kind: "version", path: "/api/v1/version" };
  // A long-tail state: a date window inside the season, unique per request.
  const window = () => {
    const y = league === "il" ? season - 1 : season;
    const day = (n) => new Date(Date.UTC(y, 9, 1) + n * 86400000).toISOString().slice(0, 10);
    const from = Math.floor(rand() * 150);
    return `&start=${day(from)}&end=${day(from + 30 + Math.floor(rand() * 90))}`;
  };
  if (r < 0.3) {
    const page = pick(PAGES), tail = page !== "home" && rand() < TAIL;
    return { kind: `page ${page}${tail ? " (tail)" : ""}`, path: null, page, league, season, ctx: tail ? window() : "" };
  }
  const reader = pick(READERS);
  let view = pick(VIEWS[reader] ?? [""]);
  if (view === "&view=shot_profile" && league !== "il") view = "";
  let ctx = reader === "player-stats" && rand() < 0.25 ? "&clutch=true" : pick(CONTEXTS);
  const tail = rand() < TAIL;
  if (tail) ctx = window();
  // The Lineups page opens with its auto minimum on (lib/tabQuery lineupsPagePath).
  if (reader === "lineups") ctx += "&auto=true";
  return { kind: `api ${reader}${tail ? " (tail)" : ""}`, league, path: `/api/v1/${league}/${reader}?season=${season}${view}${ctx}` };
}
function pathOf(a) {
  if (a.path) return a.path;
  if (a.page === "home" && a.league === "il") return `/il/home?team_id=all`;
  return `/${a.league}/${a.page}?season=${a.season}${a.ctx}`;
}

const samples = [];
async function request(vu, a, stage) {
  let path = pathOf(a);
  const v = a.path && a.league && vu.versions[a.league === "il" ? "il" : "euro"];
  if (v) path += `&v=${encodeURIComponent(v)}`;
  const t0 = performance.now();
  let status = 0, ok = false, bytes = 0, error = null, cache = null;
  try {
    const res = await fetch(base + path, {
      headers: { "x-vercel-protection-bypass": secret, ...(vu.cookie ? { cookie: vu.cookie } : {}) },
      signal: AbortSignal.timeout(DEADLINE_MS),
    });
    status = res.status;
    cache = res.headers.get("x-vercel-cache");
    const sc = res.headers.getSetCookie?.().find((c) => c.startsWith("ibpl_session="));
    if (sc) vu.cookie = sc.split(";")[0];
    const body = await res.text();
    bytes = body.length;
    if (status !== 200) error = `HTTP ${status}`;
    else if (path.startsWith("/api/")) {
      const j = JSON.parse(body);
      ok = !j.error && (Array.isArray(j.rows) || Array.isArray(j.leagues) || typeof j === "object");
      if (!ok) error = `answer error ${JSON.stringify(j.error).slice(0, 80)}`;
      else if (a.league && j.meta?.version) vu.versions[a.league === "il" ? "il" : "euro"] = j.meta.version;
    } else {
      // A page whose server read failed still renders (empty, the browser
      // fetches later), so a table page counts only if its table arrived.
      ok = body.includes("</html>") && !body.includes("Log in to Vercel");
      if (!ok) error = "page incomplete";
      else if (a.page !== "home" && !body.includes('data-column="')) {
        ok = false;
        error = "page without server data";
      }
    }
  } catch (e) {
    error = e.name === "TimeoutError" ? "timeout 30s" : `${e.name}: ${e.message}`.slice(0, 120);
  }
  samples.push({ stage, kind: a.kind, path, status, ok, bytes, cache, ms: Math.round(performance.now() - t0), error, at: Date.now() });
}

const q = (xs, p) => (xs.length ? xs[Math.min(xs.length - 1, Math.ceil(p * xs.length) - 1)] : null);
function summarize(rows) {
  const ms = rows.map((r) => r.ms).sort((a, b) => a - b);
  const failed = rows.filter((r) => !r.ok).length;
  const hits = rows.filter((r) => r.cache === "HIT").length;
  return { n: rows.length, failed, failedPct: rows.length ? +(100 * failed / rows.length).toFixed(2) : 0, p50: q(ms, 0.5), p95: q(ms, 0.95), max: ms.at(-1) ?? null, over5s: ms.filter((m) => m > 5000).length, cdnHits: hits };
}

// The target must answer before any load is put on it.
// Its versions are what a visitor's first page tells the browser.
const probe = await fetch(`${base}/api/v1/version`, { headers: { "x-vercel-protection-bypass": secret } });
const probed = await probe.json().catch(() => ({}));
if (probe.status !== 200 || !Array.isArray(probed.leagues))
  throw new Error(`${base}/api/v1/version answered HTTP ${probe.status}; not loading it`);
const firstVersions = { il: probed.leagues.find((x) => x.league === "il")?.version, euro: probed.leagues.find((x) => x.league === "euroleague")?.version };

const stages = [];
for (const users of STAGES) {
  const start = Date.now(), end = start + STAGE_MS;
  const vus = Array.from({ length: users }, () => ({ cookie: null, versions: { ...firstVersions } }));
  // Stagger arrivals over the first think time, then loop until the stage ends;
  // in-flight requests drain before the next stage starts.
  await Promise.all(vus.map(async (vu, i) => {
    await new Promise((r) => setTimeout(r, (i / users) * THINK_MS));
    while (Date.now() < end) {
      await request(vu, action(), users);
      await new Promise((r) => setTimeout(r, THINK_MS));
    }
  }));
  const rows = samples.filter((s) => s.stage === users);
  const s = { users, seconds: (Date.now() - start) / 1000, ...summarize(rows) };
  s.actionsPerSec = +(s.n / s.seconds).toFixed(2);
  stages.push(s);
  console.log(JSON.stringify(s));
  if (s.failedPct > 5 || s.p95 > 5000) { console.log(`stopping: stage ${users} over the limits`); break; }
}

const byKind = {};
for (const st of stages)
  for (const kind of [...new Set(samples.filter((r) => r.stage === st.users).map((r) => r.kind))].sort())
    (byKind[st.users] ??= {})[kind] = summarize(samples.filter((r) => r.stage === st.users && r.kind === kind));
const errors = {};
for (const r of samples.filter((r) => r.error)) errors[r.error] = (errors[r.error] ?? 0) + 1;
writeFileSync(OUT, JSON.stringify({ base, seed: 20261023, tail: TAIL, stages, byKind, errors, samples }, null, 1));

if (process.env.GITHUB_STEP_SUMMARY) {
  const md = [`## Load test: ${base} (tail ${TAIL})`, "", "| users | actions | actions/s | failed % | p50 ms | p95 ms | max ms | >5 s | CDN hits |", "|--:|--:|--:|--:|--:|--:|--:|--:|--:|",
    ...stages.map((s) => `| ${s.users} | ${s.n} | ${s.actionsPerSec} | ${s.failedPct} | ${s.p50} | ${s.p95} | ${s.max} | ${s.over5s} | ${s.cdnHits} |`),
    "", "### Errors", "", ...Object.entries(errors).map(([e, n]) => `- ${e}: ${n}`)];
  const last = stages.at(-1);
  if (last) md.push("", `### By action at ${last.users} users`, "", "| action | n | failed | p50 | p95 | max | CDN hits |", "|--|--:|--:|--:|--:|--:|--:|",
    ...Object.entries(byKind[last.users] ?? {}).map(([k, s]) => `| ${k} | ${s.n} | ${s.failed} | ${s.p50} | ${s.p95} | ${s.max} | ${s.cdnHits} |`));
  appendFileSync(process.env.GITHUB_STEP_SUMMARY, md.join("\n") + "\n");
}
