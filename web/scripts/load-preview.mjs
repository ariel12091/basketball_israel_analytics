// Closed-loop load test against a protected Vercel Preview (run by
// .github/workflows/web-preview-load.yml). Each virtual user keeps its own
// session cookie, picks seeded random actions and thinks one second between
// them; stages ramp 1 -> 150 users for STAGE_SECONDS each and stop at the
// first stage over the limits (docs/matched_local_load_protocol.md: >5% failed
// or p95 > 5 s). Read-only: GET pages and API reads, as a visitor would.
//   PREVIEW_URL, VERCEL_AUTOMATION_BYPASS_SECRET, [STAGES, STAGE_SECONDS, OUT]
import { appendFileSync, writeFileSync } from "node:fs";

const base = process.env.PREVIEW_URL;
const secret = process.env.VERCEL_AUTOMATION_BYPASS_SECRET;
if (!/^https:\/\/[a-z0-9-]+\.vercel\.app$/.test(base ?? "")) throw new Error("PREVIEW_URL");
if (!secret) throw new Error("VERCEL_AUTOMATION_BYPASS_SECRET");
const STAGES = (process.env.STAGES ?? "1,10,20,50,100,150").split(",").map(Number);
const STAGE_MS = Number(process.env.STAGE_SECONDS ?? 60) * 1000;
const OUT = process.env.OUT ?? "load-results.json";
const DEADLINE_MS = 30000, THINK_MS = 1000;

// Seeded (protocol seed 20261023), so every run asks the same sequence.
let seed = 20261023;
const rand = () => ((seed = (seed * 1103515245 + 12345) % 2147483648) / 2147483648);
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
  if (r < 0.3) return { kind: `page ${pick(PAGES)}`, path: null, league, season };
  const reader = pick(READERS);
  let view = pick(VIEWS[reader] ?? [""]);
  if (view === "&view=shot_profile" && league !== "il") view = "";
  const ctx = reader === "player-stats" && rand() < 0.25 ? "&clutch=true" : pick(CONTEXTS);
  return { kind: `api ${reader}`, path: `/api/v1/${league}/${reader}?season=${season}${view}${ctx}` };
}
function pathOf(a) {
  if (a.path) return a.path;
  const page = a.kind.slice(5);
  if (page === "home" && a.league === "il") return `/il/home?team_id=all`;
  return `/${a.league}/${page}?season=${a.season}`;
}

const samples = [];
async function request(vu, a, stage) {
  const path = pathOf(a), t0 = performance.now();
  let status = 0, ok = false, bytes = 0, error = null;
  try {
    const res = await fetch(base + path, {
      headers: { "x-vercel-protection-bypass": secret, ...(vu.cookie ? { cookie: vu.cookie } : {}) },
      signal: AbortSignal.timeout(DEADLINE_MS),
    });
    status = res.status;
    const sc = res.headers.getSetCookie?.().find((c) => c.startsWith("ibpl_session="));
    if (sc) vu.cookie = sc.split(";")[0];
    const body = await res.text();
    bytes = body.length;
    if (status !== 200) error = `HTTP ${status}`;
    else if (path.startsWith("/api/")) {
      const j = JSON.parse(body);
      ok = !j.error && (Array.isArray(j.rows) || Array.isArray(j.leagues) || typeof j === "object");
      if (!ok) error = `answer error ${JSON.stringify(j.error).slice(0, 80)}`;
    } else {
      ok = body.includes("</html>") && !body.includes("Log in to Vercel");
      if (!ok) error = "page incomplete";
    }
  } catch (e) {
    error = e.name === "TimeoutError" ? "timeout 30s" : `${e.name}: ${e.message}`.slice(0, 120);
  }
  samples.push({ stage, kind: a.kind, path, status, ok, bytes, ms: Math.round(performance.now() - t0), error, at: Date.now() });
}

const q = (xs, p) => (xs.length ? xs[Math.min(xs.length - 1, Math.ceil(p * xs.length) - 1)] : null);
function summarize(rows) {
  const ms = rows.map((r) => r.ms).sort((a, b) => a - b);
  const failed = rows.filter((r) => !r.ok).length;
  return { n: rows.length, failed, failedPct: rows.length ? +(100 * failed / rows.length).toFixed(2) : 0, p50: q(ms, 0.5), p95: q(ms, 0.95), max: ms.at(-1) ?? null, over5s: ms.filter((m) => m > 5000).length };
}

// The target must answer before any load is put on it.
const probe = await fetch(`${base}/api/v1/version`, { headers: { "x-vercel-protection-bypass": secret } });
if (probe.status !== 200 || !Array.isArray((await probe.json().catch(() => ({}))).leagues))
  throw new Error(`${base}/api/v1/version answered HTTP ${probe.status}; not loading it`);

const stages = [];
for (const users of STAGES) {
  const start = Date.now(), end = start + STAGE_MS;
  const vus = Array.from({ length: users }, () => ({ cookie: null }));
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
writeFileSync(OUT, JSON.stringify({ base, seed: 20261023, stages, byKind, errors, samples }, null, 1));

if (process.env.GITHUB_STEP_SUMMARY) {
  const md = [`## Load test: ${base}`, "", "| users | actions | actions/s | failed % | p50 ms | p95 ms | max ms | >5 s |", "|--:|--:|--:|--:|--:|--:|--:|--:|",
    ...stages.map((s) => `| ${s.users} | ${s.n} | ${s.actionsPerSec} | ${s.failedPct} | ${s.p50} | ${s.p95} | ${s.max} | ${s.over5s} |`),
    "", "### Errors", "", ...Object.entries(errors).map(([e, n]) => `- ${e}: ${n}`)];
  const last = stages.at(-1);
  if (last) md.push("", `### By action at ${last.users} users`, "", "| action | n | failed | p50 | p95 | max |", "|--|--:|--:|--:|--:|--:|",
    ...Object.entries(byKind[last.users] ?? {}).map(([k, s]) => `| ${k} | ${s.n} | ${s.failed} | ${s.p50} | ${s.p95} | ${s.max} |`));
  appendFileSync(process.env.GITHUB_STEP_SUMMARY, md.join("\n") + "\n");
}
