import { describe, it, expect } from "vitest";
import { flatten } from "./flatten";
import { loadFixture } from "../../test/gameflowGoldens";

describe("flatten gameflow", () => {
  const { answer } = loadFixture(673);
  const { rows } = flatten("gameflow", "il", answer, new URLSearchParams());
  it("gives the chat one row per run, the defining one marked", () => {
    const runs = rows.filter((r) => r.section === "run");
    expect(runs.map((r) => `${r.team} ${r.run_score} ${r.window} ${r.defining}`)).toEqual([
      "Maccabi Tel-Aviv 16-6 Q1 3:45 – Q1 0:48 no",
      "Hapoel Tel-Aviv 30-4 Q2 2:50 – Q3 4:18 no",
      "Maccabi Tel-Aviv 37-5 Q3 2:25 – Q4 2:31 yes",
    ]);
    expect(String(runs[2].explanation)).toMatch(/Estimated cause: both teams changed/);
  });
  it("and one row per player with the card's ratings", () => {
    const dib = rows.find((r) => r.section === "player" && String(r.player).includes("DIBARTOLOMEO"))!;
    expect(dib).toMatchObject({ team: "Maccabi Tel-Aviv", off_poss: 47, def_poss: 49 });
    expect(Number(dib.net)).toBeCloseTo(54.4, 1);
  });
});
