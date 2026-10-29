import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { ASK_EXAMPLES } from "./examples";

const cases = JSON.parse(readFileSync("test/ask/cases.json", "utf8")) as {
  id: string;
  turns: string[];
  expect: unknown;
}[];

it("every Ask example is a one-turn eval question, word for word", () => {
  const byId = new Map(cases.map((c) => [c.id, c]));
  for (const ex of [...ASK_EXAMPLES.il, ...ASK_EXAMPLES.euro]) {
    const c = byId.get(ex.id);
    expect(c, ex.id).toBeDefined();
    expect(c!.turns, ex.id).toEqual([ex.text]);
    expect(c!.expect, ex.id).not.toBe("unanswerable");
  }
});

it("the examples do not repeat a question", () => {
  const all = [...ASK_EXAMPLES.il, ...ASK_EXAMPLES.euro].map((e) => e.id);
  expect(new Set(all).size).toBe(all.length);
});
