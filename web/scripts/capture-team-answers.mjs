// Explicit read-only route capture; the independent Shiny oracle is untouched.
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { setTimeout } from "node:timers/promises";
const dir = "test/goldens/team_ratings";
for (const f of readdirSync(dir).filter(
  (f) => f.endsWith(".json") && !f.endsWith(".answer.json"),
)) {
  const g = JSON.parse(readFileSync(`${dir}/${f}`, "utf8"));
  const url = `http://localhost:3100/api/v1/${g.league}/team-ratings?${new URLSearchParams(g.query)}`;
  let response = await fetch(url);
  for (let retry = 1; response.status === 503 && retry <= 2; retry++) {
    console.log(g.id, "503 retry", retry);
    await response.arrayBuffer();
    await setTimeout(2000);
    response = await fetch(url);
  }
  const answer = await response.json();
  if (!response.ok)
    throw new Error(`${g.id}: ${response.status} ${JSON.stringify(answer)}`);
  writeFileSync(`${dir}/${g.id}.answer.json`, JSON.stringify(answer));
  console.log(g.id, answer.rows.length);
}
