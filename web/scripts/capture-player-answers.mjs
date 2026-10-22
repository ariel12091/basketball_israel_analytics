// Read the React API independently of the recorded Shiny expected frames.
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
const dir = "test/goldens/player_stats";
for (const file of readdirSync(dir).filter(
  (f) => f.endsWith(".json") && !f.endsWith(".answer.json"),
)) {
  const g = JSON.parse(readFileSync(`${dir}/${file}`, "utf8"));
  const res = await fetch(
    `http://localhost:3100/api/v1/${g.league}/player-stats?${new URLSearchParams(g.query)}`,
  );
  const answer = await res.json();
  if (!res.ok)
    throw new Error(`${g.id}: ${res.status} ${JSON.stringify(answer)}`);
  writeFileSync(`${dir}/${g.id}.answer.json`, JSON.stringify(answer));
  console.log(g.id, answer.rows.length);
}
