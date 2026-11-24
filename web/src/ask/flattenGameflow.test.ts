import { describe, it, expect } from "vitest";
import { flatten } from "./flatten";
import { loadFixture } from "../../test/gameflowGoldens";

describe("flatten gameflow", () => {
  const { answer } = loadFixture(673);
  const { rows } = flatten("gameflow", "il", answer, new URLSearchParams());
  it("gives the chat one row per run, the defining one marked", () => {
    const runs = rows.filter((r) => r.section === "run");
    expect(runs.map((r) => `${r.team} ${r.run_score} ${r.window} ${r.defining}`)).toEqual([
      // Ranked by win-probability swing since 2026-11-24 (was Maccabi
      // 16-6 and Hapoel 30-4 by net).
      "Hapoel Tel-Aviv 13-4 Q1 0:35 – Q2 5:39 no",
      "Hapoel Tel-Aviv 32-6 Q2 4:17 – Q3 4:18 no",
      "Maccabi Tel-Aviv 37-5 Q3 2:25 – Q4 2:31 yes",
    ]);
    // Each run's win-probability swing, from the run team's side.
    expect(runs.every((r) => /^(<1|\d+)% → (>99|\d+)%$/.test(String(r.win_chance)))).toBe(true);
    expect(String(runs[2].win_chance)).toMatch(/→ (>99|\d+)%$/);
    expect(String(runs[2].explanation)).toMatch(/Estimated cause: Hapoel Tel-Aviv's two-point shooting .* so no lineup is credited/);
  });
  it("and one row per player with the card's ratings", () => {
    const dib = rows.find((r) => r.section === "player" && String(r.player).includes("DIBARTOLOMEO"))!;
    expect(dib).toMatchObject({ team: "Maccabi Tel-Aviv", off_poss: 47, def_poss: 49 });
    expect(Number(dib.net)).toBeCloseTo(54.4, 1);
  });
  it("passes the answer's Hebrew team names to the run and player rows", () => {
    const he = {
      ...answer,
      meta: { ...answer.meta, ownTeamHe: "מכבי", oppTeamHe: "הפועל" },
      players: answer.players.map((p: { side: string }) => ({ ...p, team_name_he: p.side === "own" ? "מכבי" : "הפועל" })),
    };
    const got = flatten("gameflow", "il", he, new URLSearchParams()).rows;
    const runs = got.filter((r) => r.section === "run");
    expect(runs.map((r) => r.team_name_he)).toEqual(
      runs.map((r) => (r.team === answer.meta.ownTeam ? "מכבי" : "הפועל")),
    );
    expect(got.filter((r) => r.section === "player").every((r) => typeof r.team_name_he === "string")).toBe(true);
  });
});
