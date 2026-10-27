// Opt-in read-only verification inputs from the running local API. Expected
// frames come ONLY from Shiny's stored goldens; responses are kept separately.
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
const base = process.argv[2] ?? "http://localhost:3100";
if (!/^http:\/\/localhost:\d+$/.test(base))
  throw new Error("Use the local development server");
const dir = "test/goldens/onoff";
for (const f of readdirSync(dir).filter(
  (f) => f.endsWith(".json") && !f.endsWith(".answer.json"),
)) {
  const g = JSON.parse(readFileSync(`${dir}/${f}`, "utf8"));
  const query = new URLSearchParams(g.query);
  const response = await fetch(`${base}/api/v1/${g.league}/onoff?${query}`);
  if (!response.ok) throw new Error(`${g.id}: HTTP ${response.status}`);
  const answer = await response.json();
  let ps = [];
  if (g.filters.some((f) => f.col.startsWith("ps_"))) {
    const r = await fetch(
      `${base}/api/v1/${g.league}/player-stat-filters?${query}`,
    );
    if (!r.ok) throw new Error(`${g.id} Player Stats: HTTP ${r.status}`);
    ps = (await r.json()).rows;
  }
  writeFileSync(`${dir}/${g.id}.answer.json`, JSON.stringify({ answer, ps }));
  console.log(`${g.id}: ${answer.rows.length} source rows`);
}
