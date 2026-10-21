// Explicit read-only API capture, separate from the stored Shiny oracle.
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
const dir = "test/goldens/lineups";
const rosters = new Map(),
  logs = new Map();
for (const f of readdirSync(dir).filter(
  (f) => f.endsWith(".json") && !f.endsWith(".answer.json"),
)) {
  const g = JSON.parse(readFileSync(`${dir}/${f}`, "utf8"));
  const base = `http://localhost:3100/api/v1/${g.league}`;
  const r = await fetch(`${base}/lineups?${new URLSearchParams(g.query)}`);
  const answer = await r.json();
  if (!r.ok) throw new Error(`${g.id}: ${r.status} ${JSON.stringify(answer)}`);
  const rosterKey = `${g.league}|${g.query.season}`;
  if (!rosters.has(rosterKey)) {
    const rosterResponse = await fetch(
      `${base}/lineup-roster?season=${g.query.season}`,
    );
    const roster = await rosterResponse.json();
    if (!rosterResponse.ok)
      throw new Error(`${g.id}: roster ${rosterResponse.status}`);
    rosters.set(rosterKey, roster);
  }
  const roster = rosters.get(rosterKey);
  const first = answer.rows.find(
    (row) => !g.query.team || String(row.team_id) === g.query.team,
  );
  let games = null;
  if (first) {
    const key = `${base}|${g.view}|${first.sub_lineup_hash}`;
    if (logs.has(key)) games = logs.get(key);
    else {
      const r = await fetch(
        `${base}/lineup-games?${new URLSearchParams({ season: g.query.season, team: String(first.team_id), hash: first.sub_lineup_hash, view: g.view })}`,
      );
      games = await r.json();
      if (!r.ok) throw new Error(`${g.id}: game log ${r.status}`);
      logs.set(key, games);
    }
  }
  writeFileSync(
    `${dir}/${g.id}.answer.json`,
    JSON.stringify({ answer, roster, games }),
  );
  console.log(g.id, answer.rows.length);
}
